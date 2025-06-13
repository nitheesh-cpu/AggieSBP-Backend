# AggieRMP - Texas A&M University Rate My Professor Analysis

A comprehensive data collection and analysis system for Texas A&M University professor ratings and course information.

## 🚀 Features

- **Department & Course Scraping**: Automated collection of department and course data from TAMU College Scheduler
- **Rate My Professor Integration**: Collection and analysis of professor ratings and reviews
- **Database Management**: PostgreSQL-based storage with SQLAlchemy ORM
- **Data Analysis**: AI-powered summarization and insights generation
- **API Endpoints**: RESTful API for accessing collected data

## 📁 Project Structure

```
AggieRMP/
├── 📁 src/aggiermp/           # Main source code
│   ├── 📁 api/                # API endpoints and routes
│   ├── 📁 collectors/         # Data collection scripts
│   ├── 📁 database/           # Database models and operations
│   ├── 📁 models/             # Pydantic data models
│   ├── 📁 core/               # Core utilities and configuration
│   └── main.py                # Main application entry point
├── 📁 data/                   # Data storage
│   ├── 📁 raw/                # Raw scraped data
│   ├── 📁 processed/          # Cleaned/processed data
│   └── 📁 exports/            # Export files
├── 📁 scripts/                # Standalone scripts
├── 📁 tests/                  # Test files
├── 📁 docs/                   # Documentation
├── 📁 config/                 # Configuration files
├── pyproject.toml             # Project dependencies
└── README.md                  # This file
```

## 🛠️ Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd AggieRMP
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # macOS/Linux
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e .
   ```

4. **Set up database**
   - Configure your PostgreSQL connection
   - Run database migrations

## 🎯 Usage

### Data Collection

1. **Scrape Departments & Courses**
   ```bash
   python scripts/scrape_departments_auth.py
   ```

2. **Collect Professor Reviews**
   ```bash
   python src/aggiermp/collectors/rmp_review_collector.py
   ```

### API Server

```bash
python src/aggiermp/main.py
```

## 📊 Data Sources

- **TAMU College Scheduler**: Department and course information
- **Rate My Professor**: Professor ratings and reviews
- **Manual Curation**: Additional data validation and enhancement

## 🔧 Configuration

Configuration files are stored in the `config/` directory:
- `cookies.json`: Authentication cookies for web scraping

## 🧪 Testing

Run tests with:
```bash
python -m pytest tests/
```

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is for educational and research purposes.

## 🙋‍♂️ Support

For questions or issues, please open a GitHub issue or contact the maintainer. 