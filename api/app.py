from flask import Flask, request, jsonify
import os
import io
import traceback
import openpyxl
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

app = Flask(__name__)

# If modifying these SCOPES, delete the token.json file first
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Get credentials from environment variables only
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Map of input fields to Excel cell references
EXCEL_CELL_MAP = {
    "projectName": "D29",  # Merged cells D29:G29
    "projectNumber": "D8",  # Merged cells D8:F8
    "branch": "D6"  # Merged cells D6:G6
}

def authenticate():
    """Authenticate with Google Drive API using refresh token."""
    try:
        # Ensure required environment variables are set
        if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
            raise ValueError("Missing required environment variables for Google authentication")
            
        creds = Credentials.from_authorized_user_info({
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
            "token_uri": "https://oauth2.googleapis.com/token"
        }, SCOPES)
        return creds
    except Exception as e:
        app.logger.error(f"Authentication error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

def download_excel(service, file_id, download_path):
    """Download Excel file from Google Drive."""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(download_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        return True
    except Exception as e:
        app.logger.error(f"Download error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

def update_excel(path, input_data):
    """Update specific cells in the Excel workbook based on input data."""
    try:
        wb = openpyxl.load_workbook(path, keep_vba=True)
        
        # Check if the required sheet exists
        if 'Project Setup Form' not in wb.sheetnames:
            raise Exception("Required sheet 'Project Setup Form' not found in the Excel file")
            
        sheet = wb['Project Setup Form']

        # Check if we have any mappable data
        updates_count = 0
        for field in EXCEL_CELL_MAP.keys():
            if field in input_data and input_data[field]:
                updates_count += 1
                
        if updates_count == 0:
            app.logger.warning("No mappable data found in input. Nothing to update.")
            return False

        # Create updates dictionary from input data
        updates = {}
        for field, cell_ref in EXCEL_CELL_MAP.items():
            if field in input_data and input_data[field]:
                updates[cell_ref] = input_data[field]
                app.logger.info(f"Updating {field} in cell {cell_ref} with value: {input_data[field]}")

        # Apply all updates
        for cell_ref, value in updates.items():
            sheet[cell_ref] = value

        wb.save(path)
        return True
        
    except Exception as e:
        app.logger.error(f"Excel update error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

def upload_excel(service, file_id, path):
    """Upload updated Excel file back to Google Drive."""
    try:
        media = MediaFileUpload(
            path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        updated_file = service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        return updated_file
    except Exception as e:
        app.logger.error(f"Upload error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

# Add a handler for the root path
@app.route('/', methods=['GET', 'POST'])
def root():
    """Root endpoint that handles webhook requests from Zapier"""
    if request.method == 'POST':
        try:
            # Debug logging for request inspection
            app.logger.info(f"Raw request data: {request.get_data(as_text=True)}")
            app.logger.info(f"Request form: {request.form}")
            app.logger.info(f"Request headers: {dict(request.headers)}")
            
            # Try to get data from different content types
            data = {}
            content_type = request.headers.get('Content-Type', '')
            app.logger.info(f"Received webhook with Content-Type: {content_type}")
            
            if 'application/json' in content_type:
                if request.data:
                    try:
                        data = request.json
                    except:
                        app.logger.warning("Failed to parse JSON, trying form data")
                        data = request.form.to_dict()
            elif 'application/x-www-form-urlencoded' in content_type:
                data = request.form.to_dict()
            else:
                # Log raw data for debugging
                app.logger.info(f"Raw data: {request.data}")
                # Try to parse as form data
                data = request.form.to_dict()
                if not data:
                    # If still empty, try to get raw data
                    data = request.get_data(as_text=True)
                    app.logger.info(f"Received raw data: {data}")
            
            # Special handling for quoted keys in form data
            if isinstance(data, dict):
                # Handle quoted keys by creating versions without quotes
                new_data = {}
                for key, value in data.items():
                    # Remove quotes from keys if present
                    clean_key = key.strip('"')
                    new_data[clean_key] = value
                data = new_data
                
            app.logger.info(f"Processed webhook data: {data}")
            
            # Process the same way as /update-excel
            if not data:
                return jsonify({"status": "error", "message": "No data provided"}), 400
                
            # Get the file ID
            file_id = data.get('Current File ID')
            if not file_id:
                return jsonify({"status": "error", "message": "No file ID provided"}), 400
                
            # Set up temporary file path
            temp_file = f"/tmp/{file_id}.xlsx"
            
            # Authenticate with Google Drive
            creds = authenticate()
            service = build('drive', 'v3', credentials=creds)
            
            # Download the file
            download_excel(service, file_id, temp_file)
            
            # Update the Excel file
            update_success = update_excel(temp_file, data)
            
            if update_success:
                # Upload the updated file
                upload_excel(service, file_id, temp_file)
                
            # Clean up
            if os.path.exists(temp_file):
                os.remove(temp_file)
                
            return jsonify({
                "status": "success",
                "message": "Excel file updated successfully" if update_success else "No updates were made"
            })
            
        except Exception as e:
            app.logger.error(f"Error processing webhook request: {str(e)}")
            app.logger.error(traceback.format_exc())
            
            # Clean up in case of error
            if 'temp_file' in locals() and os.path.exists(temp_file):
                os.remove(temp_file)
                
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # Handle GET requests (like health checks)
    return jsonify({"status": "ok", "message": "API is running"}), 200

@app.route('/update-excel', methods=['POST'])
def update_excel_api():
    """API endpoint to update Excel file on Google Drive"""
    try:
        # Get the JSON data from the request
        data = request.json
        app.logger.info(f"Received request with data: {data}")
        
        # Validate the input
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
            
        # Get the file ID
        file_id = data.get('Current File ID')
        if not file_id:
            return jsonify({"status": "error", "message": "No file ID provided"}), 400
            
        # Set up temporary file path
        temp_file = f"/tmp/{file_id}.xlsx"
        
        # Authenticate with Google Drive
        creds = authenticate()
        service = build('drive', 'v3', credentials=creds)
        
        # Download the file
        download_excel(service, file_id, temp_file)
        
        # Update the Excel file
        update_success = update_excel(temp_file, data)
        
        if update_success:
            # Upload the updated file
            upload_excel(service, file_id, temp_file)
            
        # Clean up
        if os.path.exists(temp_file):
            os.remove(temp_file)
            
        return jsonify({
            "status": "success",
            "message": "Excel file updated successfully" if update_success else "No updates were made"
        })
        
    except Exception as e:
        app.logger.error(f"Error processing request: {str(e)}")
        app.logger.error(traceback.format_exc())
        
        # Clean up in case of error
        if 'temp_file' in locals() and os.path.exists(temp_file):
            os.remove(temp_file)
            
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    # For local development only - use proper WSGI server in production
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))