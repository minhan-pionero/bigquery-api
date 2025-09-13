# Multi-Platform Social Scraper BigQuery API Server

Fast API server for social media extensions to interact with BigQuery (LinkedIn, Facebook, etc.)

## 🚀 Quick Start

### Windows

1. **First time setup:**
   ```bash
   setup.bat
   ```

2. **Start the server:**
   ```bash
   start.bat
   ```

3. **For development (with auto-reload):**
   ```bash
   dev.bat
   ```

## 📋 Available Scripts

| Script | Description |
|--------|-------------|
| `setup.bat` | Initial environment setup |
| `start.bat` | Start production server |
| `quick_start.bat` | One-click setup and start |

## 🔧 Prerequisites

- **Python 3.8+** - Download from [python.org](https://www.python.org/downloads/)

## 🚀 Quick Start

### Method 1: One-Click Start (Recommended)
```bash
# Setup and run in one command
quick_start.bat
```

### Method 2: Manual Setup
```bash
# 1. Setup Python environment
setup.bat

# 2. Start the server
start.bat
```

## 🌐 API Endpoints

Once the server is running:

- **Server:** http://localhost:8000
- **API Documentation:** http://localhost:8000/docs
- **Health Check:** http://localhost:8000/health

### Main Endpoints

#### LinkedIn
- `GET /linkedin/profiles/pending` - Get pending profiles
- `POST /linkedin/profiles/batch` - Insert profiles batch
- `GET /linkedin/keywords/pending` - Get pending keywords
- `GET /linkedin/stats` - Get LinkedIn statistics

#### Facebook
- `GET /facebook/profiles/pending` - Get pending profiles
- `POST /facebook/profiles/batch` - Insert profiles batch
- `GET /facebook/seed-urls/pending` - Get pending seed URLs
- `GET /facebook/stats` - Get Facebook statistics

#### Facebook V1 (Alternative tables)
- `GET /facebook/profile-urls-v1/pending` - Get pending profile URLs V1
- `GET /facebook/seed-urls-v1/pending` - Get pending seed URLs V1

## 📁 Project Structure

```
api/
├── start.bat              # Windows start script
├── setup.bat              # Environment setup script
├── quick_start.bat        # One-click setup and start
├── run_server.py          # Server entry point
├── main.py                # FastAPI application
├── requirements.txt       # Python dependencies
├── config/
│   ├── settings.py        # Configuration settings
│   └── schemas.py         # BigQuery table schemas
├── api/
│   ├── linkedin.py        # LinkedIn API endpoints
│   ├── facebook.py        # Facebook API endpoints
│   └── email.py           # Email API endpoints
├── services/
│   ├── bigquery_service.py # BigQuery operations
│   └── email_service.py   # Email operations
└── utils/
    └── transformers.py    # Data transformation utilities
```

## 🧪 Testing

### Manual Testing
```powershell
# Test if server is running
Invoke-WebRequest http://localhost:8000/health

# Test LinkedIn stats
Invoke-WebRequest http://localhost:8000/linkedin/stats

# Test Facebook stats
Invoke-WebRequest http://localhost:8000/facebook/stats
```

### Automated Testing
```bash
test_api.bat  # Windows
## 🚨 Troubleshooting

### Common Issues

1. **"Python not found"**
   - Install Python 3.8+ from python.org
   - Make sure Python is added to PATH

2. **"Port already in use"**
   - Change port in run_server.py
   - Check which process is using port 8000

3. **"Permission denied"**
   - Run terminal as Administrator
   - Check antivirus settings
   - Kill existing processes using port 8000

4. **"Dependencies installation failed"**
   - Check internet connection
   - Update pip: `python -m pip install --upgrade pip`
   - Try installing dependencies individually

4. **"Virtual environment creation failed"**
   - Ensure you have write permissions
   - Try running as Administrator

### Logs
- Server logs are displayed in the console
- For detailed debugging, check console output

## 📞 Support

For issues or questions:
1. Check the API documentation at http://localhost:8000/docs
2. Review the logs in the console

## 🔄 Updates

To update the server:
1. Pull latest changes  
2. Run `setup.bat` to update dependencies
3. Restart the server with `start.bat`