from flask import Flask, request, jsonify
import os
import json
import traceback
import openpyxl
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2 import service_account
import time
import signal
from functools import wraps

app = Flask(__name__)

# Set request timeout (in seconds)
REQUEST_TIMEOUT = 30

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive']

# Map of input fields to Excel cell references
EXCEL_CELL_MAP = {
    "projectName": "D29",  # Merged cells D29:G29
    "projectNumber": "D8",  # Merged cells D8:F8
    "branch": "D6"  # Merged cells D6:G6
}

def timeout_handler(signum, frame):
    raise TimeoutError("Request timed out")

def timeout_decorator(timeout_seconds):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Set the timeout handler
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                # Disable the alarm
                signal.alarm(0)
        return wrapper
    return decorator

def authenticate():
    """Authenticate with Google Drive API using service account."""
    try:
        # Get service account JSON from environment variable
        service_account_info = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"))
        app.logger.info(f"Authenticating with service account: {service_account_info.get('client_email', 'unknown')}")
        
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES)
        return credentials
    except Exception as e:
        app.logger.error(f"Authentication error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

@timeout_decorator(REQUEST_TIMEOUT)
def download_excel(service, file_id, download_path):
    """Download Excel file from Google Drive with timeout handling."""
    try:
        # Add support for shared drives
        app.logger.info(f"About to download file with ID: '{file_id}'")
        
        try:
            file_metadata = service.files().get(
                fileId=file_id,
                supportsAllDrives=True
            ).execute()
            app.logger.info(f"File exists, name: {file_metadata.get('name')}")
        except Exception as e:
            app.logger.error(f"File metadata check failed: {str(e)}")
            raise ValueError(f"Unable to access file with ID {file_id}. Make sure the file exists and is shared with the service account.")
            
        request = service.files().get_media(
            fileId=file_id,
            supportsAllDrives=True
        )
        
        # Use context manager to ensure file is properly closed
        with open(download_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                app.logger.info(f"Download progress: {int(status.progress() * 100)}%")
                
        # Verify file is valid
        if os.path.getsize(download_path) < 1000:  # Check for minimum size
            raise ValueError(f"Downloaded file appears corrupt (too small): {os.path.getsize(download_path)} bytes")
            
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
        
        # Verify file was saved properly
        verify_wb = openpyxl.load_workbook(path, keep_vba=True)
        verify_wb.close()
        
        return True
        
    except Exception as e:
        app.logger.error(f"Excel update error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

@timeout_decorator(REQUEST_TIMEOUT)
def upload_excel(service, file_id, path):
    """Upload updated Excel file back to Google Drive with timeout handling."""
    try:
        # Add support for shared drives
        file_metadata = service.files().get(
            fileId=file_id, 
            supportsAllDrives=True
        ).execute()
        app.logger.info(f"File metadata before upload: {file_metadata}")
        app.logger.info(f"File ID being used for API call: '{file_id}'")

        # Log file details
        file_size = os.path.getsize(path)
        app.logger.info(f"Local file size: {file_size} bytes")

        # Use smaller chunk size for uploads
        media = MediaFileUpload(
            path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True,
            chunksize=1024*1024  # 1MB chunks
        )
        
        app.logger.info(f"Attempting chunked upload")
        app.logger.info(f"Attempting to upload file to ID: {file_id}")

        request = service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        )
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                app.logger.info(f"Upload progress: {int(status.progress() * 100)}%")
        
        app.logger.info(f"Upload successful. Updated file details: {response}")
        return response
    except Exception as e:
        app.logger.error(f"Upload error: {str(e)}")
        app.logger.error(traceback.format_exc())
        raise

@app.route('/', methods=['GET', 'POST'])
def root():
    """Root endpoint that handles webhook requests from Zapier"""
    if request.method == 'POST':
        try:
            start_time = time.time()
            app.logger.info("Received webhook request")
            
            # Debug logging for request inspection
            raw_data = request.get_data(as_text=True)
            app.logger.info(f"Raw request data: {raw_data}")
            app.logger.info(f"Request form: {request.form}")
            app.logger.info(f"Request headers: {dict(request.headers)}")
            
            # Initialize data dictionary
            data = {}
            
            # First check if we have form data
            if request.form:
                data = request.form.to_dict()
                app.logger.info(f"Parsed form data: {data}")
            # Then check for URL-encoded data with mismatched Content-Type
            elif "=" in raw_data and "&" in raw_data:
                # Parse URL-encoded data manually
                from urllib.parse import parse_qs
                parsed_data = parse_qs(raw_data)
                # Convert from lists to single values
                data = {k: v[0] for k, v in parsed_data.items()}
                app.logger.info(f"Parsed URL-encoded data: {data}")
            # Finally try JSON parsing
            elif raw_data:
                try:
                    data = request.json
                    app.logger.info(f"Parsed JSON data: {data}")
                except:
                    app.logger.warning("Failed to parse as JSON")
            
            app.logger.info(f"Processed webhook data: {data}")
            
            # Process the webhook data
            if not data:
                return jsonify({"status": "error", "message": "No data provided"}), 400
                
            # Get the file ID
            file_id = data.get('Current File ID')
            if not file_id:
                return jsonify({"status": "error", "message": "No file ID provided"}), 400

            app.logger.info(f"Original file ID received: '{file_id}'")
                
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
                
            end_time = time.time()
            app.logger.info(f"Request completed in {end_time - start_time:.2f} seconds")
                
            return jsonify({
                "status": "success",
                "message": "Excel file updated successfully" if update_success else "No updates were made"
            })
            
        except TimeoutError as e:
            app.logger.error(f"Request timed out: {str(e)}")
            return jsonify({
                "status": "error",
                "message": "Request timed out. Please try again."
            }), 504
            
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

@app.route('/test_file_access', methods=['GET'])
def test_file_access():
    file_id = request.args.get('file_id')
    app.logger.info(f"Testing file access for ID: '{file_id}'")
    try:
        creds = authenticate()
        service = build('drive', 'v3', credentials=creds)
        file_metadata = service.files().get(fileId=file_id, supportsAllDrives=True).execute()
        return jsonify({"success": True, "file_name": file_metadata.get('name')})
    except Exception as e:
        app.logger.error(f"Test file access error: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/list_files', methods=['GET'])
def list_files():
    try:
        creds = authenticate()
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            pageSize=10,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        files = results.get('files', [])
        return jsonify({"files": files})
    except Exception as e:
        app.logger.error(f"List files error: {str(e)}")
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    # For local development only - use proper WSGI server in production
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))