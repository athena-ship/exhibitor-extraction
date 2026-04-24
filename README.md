# Exhibitor Extraction Web App

A web application for processing trade show exhibitor lists, enriching contacts via Seamless.ai API, and outputting XLSX files to Google Drive.

## Architecture

```
Frontend (React/Vite) → FastAPI Backend → 
  1. Google Sheets API (read pending requests)
  2. Exhibitor extraction (existing Python skill)
  3. Seamless.ai API (contact lookup with rate limiting)
  4. XLSX generation (openpyxl)
  5. Google Drive API (save output files)
```

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 20+
- Tesseract OCR (for floorplan extraction)

### Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SEAMLESS_API_KEY="your-seamless-api-key"
export GOOGLE_SERVICE_ACCOUNT_EMAIL="your-service-account@project.iam.gserviceaccount.com"
export GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"

# Run the server
uvicorn main:app --reload --port 8000
```

### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `SEAMLESS_API_KEY` | Seamless.ai API key for contact enrichment |
| `GOOGLE_SERVICE_ACCOUNT_EMAIL` | Google service account email |
| `GOOGLE_PRIVATE_KEY` | Google service account private key (with `\n` for newlines) |

## API Endpoints

### `GET /api/pending`

List pending requests from the Google Sheet.

**Response:**
```json
[
  {
    "row_index": 1,
    "show_name": "Anime Expo 2025",
    "start_date": "07/04/25",
    "location": "Los Angeles, CA",
    "floorplan_url": "https://...",
    "exhibitor_list_url": "https://..."
  }
]
```

### `POST /api/process`

Start processing selected requests.

**Request:**
```json
{
  "row_indices": [1, 2, 3]
}
```

**Response:**
```json
{
  "job_id": "uuid-here"
}
```

### `GET /api/status/{job_id}`

Check processing status.

**Response:**
```json
{
  "job_id": "uuid-here",
  "status": "processing",
  "progress": 45,
  "message": "Enriching contacts for: Anime Expo 2025",
  "results": null,
  "error": null
}
```

### `GET /api/health`

Health check endpoint.

## Processing Flow

1. **Read pending rows** from Google Sheet where `Delivered` is blank
2. **Extract exhibitors** from floorplan URL or exhibitor list URL
   - Priority: HTML exhibitor list → PDF floorplan → Image floorplan
3. **Enrich contacts** via Seamless.ai API
   - Tiered title search: Event roles → Marketing/Sales → Executives
   - Rate limited to 60 requests/minute
4. **Generate XLSX** using the AE_ShowList_Template.xlsx template
5. **Upload to Google Drive** and update the tracking sheet

## Deployment (Render)

1. Push code to GitHub
2. Create new Blueprint on Render
3. Connect repository
4. Set environment variables in Render dashboard
5. Deploy

## File Naming Convention

Output files are named: `YYYY_MM_Show Name Year, City, ST.xlsx`

Examples:
- `2026_07_Anime Expo 2025, Los Angeles, CA.xlsx`
- `2026_10_International GSE Expo 2025, Las Vegas, NV.xlsx`

## Reference

- **Google Sheet ID**: `1Yhamt9si8Hs64g0q4JVwlEyghXsgDUXT`
- **Drive Folder ID**: `17xvwirTNTfH5HmKmXIY7JYW_XoxJ1T-5`
- **Template**: `templates/AE_ShowList_Template.xlsx`
- **Floorplan Skill**: `~/.openclaw/agents/engineering/floorplan-skill/`

## License

Private - Absolute Exhibits
