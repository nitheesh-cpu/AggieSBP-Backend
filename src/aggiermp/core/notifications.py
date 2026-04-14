import json
import logging
from typing import Dict, Any, List
from pywebpush import webpush, WebPushException
from sqlalchemy.orm import Session

from ..database.base import UserSubscriptionDB
from ..core.config import settings

logger = logging.getLogger(__name__)

class NotificationService:
    @staticmethod
    def send_push_to_user(user_id: str, message: Dict[str, Any], db: Session) -> bool:
        """
        Send a web push notification to all active subscriptions for a user.
        Removes invalid subscriptions (404/410).
        """
        if not settings.vapid_private_key:
            logger.error("VAPID private key not configured")
            return False

        subscriptions = db.query(UserSubscriptionDB).filter(UserSubscriptionDB.user_id == user_id).all()
        
        if not subscriptions:
            logger.info(f"No active subscriptions found for user {user_id}")
            return False

        success_count = 0
        payload = json.dumps(message)

        for sub in subscriptions:
            subscription_info = {
                "endpoint": sub.endpoint,
                "keys": {
                    "p256dh": sub.p256dh,
                    "auth": sub.auth
                }
            }
            
            try:
                webpush(
                    subscription_info=subscription_info,
                    data=payload,
                    vapid_private_key=settings.vapid_private_key,
                    vapid_claims={"sub": settings.vapid_contact_email}
                )
                success_count += 1
            except WebPushException as ex:
                logger.error(f"Web Push Error: {repr(ex)}")
                # If the subscription is expired or invalid, remove it
                if ex.response and ex.response.status_code in [404, 410]:
                    logger.info(f"Removing invalid subscription for user {user_id}")
                    db.delete(sub)
                    db.commit()
            except Exception as e:
                logger.error(f"Failed to send push notification: {str(e)}")

        return success_count > 0
