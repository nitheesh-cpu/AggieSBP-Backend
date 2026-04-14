from typing import List, Optional, Union
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
import json

from supertokens_python.recipe.session import SessionContainer
from supertokens_python.recipe.session.framework.fastapi import verify_session
import uuid
from pydantic import ConfigDict

from ...database.base import get_session, UserSubscriptionDB
from ...models.schema import UserSchedule, UserTrackedSection, UserSubscription
from ...core.notifications import NotificationService

router: APIRouter = APIRouter(prefix="/users", tags=["users"])

# Request Models
class PushSubscriptionRequest(BaseModel):
    endpoint: str
    p256dh: str
    auth: str
    device_name: Optional[str] = None
    user_agent: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)

class CreateScheduleRequest(BaseModel):
    name: str
    term_code: str
    courses: List[Union[str, int]] = []

class CreateTrackingRequest(BaseModel):
    section_id: str
    term_code: str


class PushSubscriptionDeleteRequest(BaseModel):
    endpoint: str


class PushSubscriptionDevice(BaseModel):
    id: str
    endpoint: str
    device_name: Optional[str] = None
    user_agent: Optional[str] = None
    created_at: Optional[str] = None
    last_seen_at: Optional[str] = None


@router.post("/push-subscription")
async def save_push_subscription(
    request: PushSubscriptionRequest,
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session)
):
    """Save or update a web push subscription for the user."""
    user_id = session.get_user_id()
    
    try:
        # Check if this exact endpoint already exists for this user to avoid duplicates
        query = text("""
            SELECT id FROM user_subscriptions 
            WHERE user_id = :user_id AND endpoint = :endpoint
        """)
        result = db.execute(query, {
            "user_id": user_id,
            "endpoint": request.endpoint
        })
        existing = result.fetchone()
        
        if existing:
            # Update existing subscription's keys (maybe they rotated)
            update_query = text("""
                UPDATE user_subscriptions 
                SET p256dh = :p256dh,
                    auth = :auth,
                    device_name = :device_name,
                    user_agent = :user_agent,
                    last_seen_at = NOW()
                WHERE id = :id
            """)
            db.execute(update_query, {
                "id": existing.id,
                "p256dh": request.p256dh,
                "auth": request.auth,
                "device_name": request.device_name,
                "user_agent": request.user_agent,
            })
        else:
            # Insert new subscription
            insert_query = text("""
                INSERT INTO user_subscriptions (
                    id, user_id, endpoint, p256dh, auth, device_name, user_agent, last_seen_at
                )
                VALUES (
                    :id, :user_id, :endpoint, :p256dh, :auth, :device_name, :user_agent, NOW()
                )
            """)
            db.execute(insert_query, {
                "id": str(uuid.uuid4()),
                "user_id": user_id,
                "endpoint": request.endpoint,
                "p256dh": request.p256dh,
                "auth": request.auth,
                "device_name": request.device_name,
                "user_agent": request.user_agent,
            })
            
        db.commit()
        return {"status": "success", "message": "Push subscription saved"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.delete("/push-subscription")
async def delete_push_subscription(
    request: PushSubscriptionDeleteRequest,
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session),
):
    """Remove a web push subscription for the current user (single device)."""
    user_id = session.get_user_id()

    try:
        query = text(
            """
            DELETE FROM user_subscriptions
            WHERE user_id = :user_id AND endpoint = :endpoint
        """
        )
        result = db.execute(
            query, {"user_id": user_id, "endpoint": request.endpoint}
        )
        db.commit()

        if result.rowcount == 0:
            # Not fatal; just means no matching subscription in DB
            return {
                "status": "not_found",
                "message": "No matching subscription for this device",
            }

        return {
            "status": "success",
            "message": "Push subscription removed for this device",
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Database error: {str(e)}"
        )


@router.get("/push-subscriptions", response_model=List[PushSubscriptionDevice])
async def list_push_subscriptions(
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session),
):
    """List current user's registered push subscription endpoints."""
    user_id = session.get_user_id()

    try:
        query = text(
            """
            SELECT id, endpoint, device_name, user_agent, created_at, last_seen_at
            FROM user_subscriptions
            WHERE user_id = :user_id
            ORDER BY COALESCE(last_seen_at, created_at) DESC
            """
        )
        result = db.execute(query, {"user_id": user_id})
        rows = result.fetchall()

        return [
            PushSubscriptionDevice(
                id=str(row.id),
                endpoint=row.endpoint,
                device_name=row.device_name,
                user_agent=row.user_agent,
                created_at=row.created_at.isoformat() if row.created_at else None,
                last_seen_at=row.last_seen_at.isoformat() if row.last_seen_at else None,
            )
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/schedules", response_model=UserSchedule)
async def create_schedule(
    request: CreateScheduleRequest,
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session)
):
    """Save a user schedule."""
    user_id = session.get_user_id()
    
    try:
        query = text("""
            INSERT INTO user_schedules (user_id, name, term_code, courses)
            VALUES (:user_id, :name, :term_code, :courses)
            RETURNING id, user_id, name, term_code, courses, created_at
        """)
        
        result = db.execute(query, {
            "user_id": user_id,
            "name": request.name,
            "term_code": request.term_code,
            "courses": json.dumps(request.courses)
        })
        db.commit()
        
        row = result.fetchone()
        return UserSchedule(**row._mapping)
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/schedules", response_model=List[UserSchedule])
async def get_schedules(
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session)
):
    """Get all saved schedules for the authenticated user."""
    user_id = session.get_user_id()
    
    try:
        query = text("""
            SELECT id, user_id, name, term_code, courses, created_at
            FROM user_schedules
            WHERE user_id = :user_id
            ORDER BY created_at DESC
        """)
        
        result = db.execute(query, {"user_id": user_id})
        rows = result.fetchall()
        
        return [UserSchedule(**row._mapping) for row in rows]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post("/tracking", response_model=UserTrackedSection)
async def track_section(
    request: CreateTrackingRequest,
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session)
):
    """Track a section for the authenticated user."""
    user_id = session.get_user_id()
    
    try:
        # Optimized: atomic insert with conflict handling (id required by table)
        new_id = str(uuid.uuid4())
        query = text("""
            INSERT INTO user_tracked_sections (id, user_id, section_id, term_code, status)
            VALUES (:id, :user_id, :section_id, :term_code, 'active')
            ON CONFLICT (user_id, section_id) DO NOTHING
            RETURNING id, user_id, section_id, term_code, status, created_at
        """)
        
        result = db.execute(query, {
            "id": new_id,
            "user_id": user_id,
            "section_id": request.section_id,
            "term_code": request.term_code
        })
        db.commit()
        
        row = result.fetchone()
        
        # Add to Redis for real-time polling
        try:
            from ..core.cache import get_redis
            redis_client = await get_redis()
            if redis_client:
                # Parse CRN from section_id (e.g., 202631-12345-ACCT-209-500)
                # Assuming format is always {term}-{crn}-...
                parts = request.section_id.split('-')
                if len(parts) >= 2:
                    crn = parts[1]
                    term = request.term_code
                    # Add to master list of sections to poll
                    await redis_client.sadd("tracked_sections", f"{term}:{crn}")
                    # Add user to list of listeners for this section
                    await redis_client.sadd(f"trackers:{term}:{crn}", user_id)
        except Exception as e:
            # Don't fail request if Redis fails, just log it
            print(f"Redis tracking error: {e}")

        if not row:
            # If no row returned, it means conflict (already exists).
            # Return the existing record.
            return await get_tracked_section(request.section_id, session, db)
            
        return UserTrackedSection(**row._mapping)
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/tracking", response_model=List[UserTrackedSection])
async def get_tracked_sections(
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session)
):
    """Get all tracked sections for the authenticated user."""
    user_id = session.get_user_id()
    
    try:
        query = text("""
            SELECT id, user_id, section_id, term_code, status, created_at
            FROM user_tracked_sections
            WHERE user_id = :user_id
            ORDER BY created_at DESC
        """)
        
        result = db.execute(query, {"user_id": user_id})
        rows = result.fetchall()
        
        return [UserTrackedSection(**row._mapping) for row in rows]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.delete("/tracking/{section_id}")
async def stop_tracking(
    section_id: str,
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session)
):
    """Stop tracking a section."""
    user_id = session.get_user_id()
    
    try:
        query = text("""
            DELETE FROM user_tracked_sections
            WHERE user_id = :user_id AND section_id = :section_id
        """)
        
        result = db.execute(query, {"user_id": user_id, "section_id": section_id})
        db.commit()
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Tracked section not found")
            
        return {"message": "Tracking removed"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def get_tracked_section(section_id: str, session, db):
    user_id = session.get_user_id()
    query = text("""
        SELECT id, user_id, section_id, term_code, status, created_at
        FROM user_tracked_sections
        WHERE user_id = :user_id AND section_id = :section_id
    """)
    result = db.execute(query, {"user_id": user_id, "section_id": section_id})
    row = result.fetchone()
    if row:
        return UserTrackedSection(**row._mapping)
    # Fallback if not found immediately after check (race condition weirdness)
    raise HTTPException(status_code=404, detail="Tracked section not found")


@router.post("/test-notification")
async def send_test_notification(
    session: SessionContainer = Depends(verify_session()),
    db: Session = Depends(get_session),
):
    """
    Send a simple test push notification to the current user's active subscriptions.
    """
    user_id = session.get_user_id()

    message = {
        "title": "AggieSB+ Test Alert",
        "body": "If you see this, push notifications are working on this device.",
        "url": "https://tamu.collegescheduler.com/terms",
    }

    try:
        sent = NotificationService.send_push_to_user(user_id, message, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send notification: {str(e)}")

    if not sent:
        raise HTTPException(
            status_code=400,
            detail="No active push subscriptions found for this user. Make sure notifications are enabled.",
        )

    return {"status": "success", "message": "Test notification sent"}
