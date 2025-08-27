import os
import re
import flask
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import pandas as pd
import io
import time
import logging
from werkzeug.utils import secure_filename

app = flask.Flask(__name__)
app.secret_key = 'your-super-secret-key-change-me'  # Thay đổi key này

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cấu hình xác thực
# Thử nhiều đường dẫn khác nhau cho credentials.json
def get_credentials_file():
    # Kiểm tra môi trường để chọn file credentials phù hợp
    local_credentials_paths = [
        'credentials_local.json',
        os.path.join(os.path.dirname(__file__), 'credentials_local.json'),
        os.path.abspath('credentials_local.json')
    ]
    
    # Ưu tiên credentials_local.json cho môi trường local
    for path in local_credentials_paths:
        if os.path.exists(path):
            logger.info(f"Using local credentials file: {path}")
            return path
    
    # Fallback về credentials.json cho PythonAnywhere
    production_credentials_paths = [
        'credentials.json',
        os.path.join(os.path.dirname(__file__), 'credentials.json'),
        '/home/111101/mysite/credentials.json',
        os.path.abspath('credentials.json')
    ]
    
    for path in production_credentials_paths:
        if os.path.exists(path):
            logger.info(f"Using production credentials file: {path}")
            return path
    
    raise FileNotFoundError(f"No credentials file found in any of these paths: {local_credentials_paths + production_credentials_paths}")

CLIENT_SECRETS_FILE = get_credentials_file()
SCOPES = ['https://www.googleapis.com/auth/drive']

# Cấu hình upload file
UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_id_from_url(url):
    """Trích xuất ID của file hoặc thư mục từ URL Google Drive."""
    if not url:
        return None
    
    # Các pattern phổ biến cho Google Drive URLs
    patterns = [
        r'/folders/([a-zA-Z0-9_-]+)',
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'^([a-zA-Z0-9_-]+)$'  # Chỉ ID thuần
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def count_files_recursively(service, folder_id):
    """Đếm tổng số file trong một thư mục và các thư mục con."""
    count = 0
    try:
        page_token = None
        while True:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(
                q=query, 
                pageSize=1000, 
                fields="nextPageToken, files(id, mimeType)",
                pageToken=page_token
            ).execute()
            
            items = results.get('files', [])
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    count += count_files_recursively(service, item['id'])
                else:
                    count += 1
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
                
    except HttpError as e:
        logger.error(f"Error counting files in folder {folder_id}: {e}")
    return count

def safe_file_operation(operation, *args, **kwargs):
    """Wrapper để thực hiện các thao tác file một cách an toàn với retry."""
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            return operation(*args, **kwargs)
        except HttpError as e:
            if e.resp.status in [403, 429, 500, 502, 503, 504]:  # Rate limit hoặc server errors
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
            raise e
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise e

def read_excel_file(file_path):
    """Đọc file Excel và trả về list các URL pairs."""
    try:
        # Thử đọc như Excel file trước
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # Lấy 2 cột đầu tiên
        if len(df.columns) < 1:
            raise ValueError("File Excel phải có ít nhất 1 cột (source URLs)")
        
        urls = []
        for index, row in df.iterrows():
            source_url = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
            dest_url = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ""
            
            if source_url and source_url != "nan":
                urls.append((source_url, dest_url if dest_url != "nan" else None))
        
        return urls
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        raise e

# Routes
@app.route('/')
def index():
    """Trang chủ."""
    logged_in = 'credentials' in flask.session
    
    # Thử nhiều đường dẫn khác nhau cho index.html
    template_paths = [
        'index.html',
        os.path.join(os.path.dirname(__file__), 'index.html'),
        '/home/111101/mysite/index.html',
        os.path.abspath('index.html')
    ]
    
    for template_path in template_paths:
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                template_content = f.read()
            return flask.render_template_string(template_content, logged_in=logged_in)
        except FileNotFoundError:
            continue
    
    # Nếu không tìm thấy file nào, trả về template đơn giản
    logger.error(f"Template file not found in any of these paths: {template_paths}")
    return flask.render_template_string(
        '''<!DOCTYPE html>
        <html>
        <head>
            <title>Google Drive Copier Pro</title>
            <meta charset="utf-8">
        </head>
        <body>
            <h1>Google Drive Copier Pro</h1>
            <p>Ứng dụng đang chạy nhưng không tìm thấy file template.</p>
            {% if not logged_in %}
                <a href="/login">Đăng nhập Google</a>
            {% else %}
                <p>Đã đăng nhập thành công!</p>
                <a href="/logout">Đăng xuất</a>
            {% endif %}
        </body>
        </html>''', 
        logged_in=logged_in
    )

@app.route('/login')
def login():
    """Khởi tạo quá trình đăng nhập OAuth2."""
    try:
        # Thử lấy đường dẫn credentials.json
        try:
            credentials_file = get_credentials_file()
        except FileNotFoundError as e:
            logger.error(f"Credentials file not found: {e}")
            return flask.render_template_string(
                '<h1>Lỗi cấu hình</h1><p>Không tìm thấy file credentials.json. Vui lòng kiểm tra cấu hình.</p>'
            )
        
        flow = Flow.from_client_secrets_file(
            credentials_file, scopes=SCOPES,
            redirect_uri=flask.url_for('oauth2callback', _external=True)
        )
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'  # Buộc hiển thị consent screen để có refresh_token
        )
        flask.session['state'] = state
        return flask.redirect(authorization_url)
    except Exception as e:
        logger.error(f"Login error: {e}")
        return flask.render_template_string(
            f'<h1>Lỗi đăng nhập</h1><p>Không thể khởi tạo quá trình đăng nhập: {str(e)}</p>'
        )

@app.route('/oauth2callback')
def oauth2callback():
    try:
        # Kiểm tra state có tồn tại trong session không
        if 'state' not in flask.session:
            logger.error("No state found in session during OAuth callback")
            return flask.render_template_string(
                '<h1>Lỗi xác thực</h1><p>Session đã hết hạn. Vui lòng <a href="/login">đăng nhập lại</a>.</p>'
            )
        
        state = flask.session['state']
        
        # Thử lấy đường dẫn credentials.json
        try:
            credentials_file = get_credentials_file()
        except FileNotFoundError as e:
            logger.error(f"Credentials file not found during OAuth callback: {e}")
            return flask.render_template_string(
                '<h1>Lỗi cấu hình</h1><p>Không tìm thấy file credentials.json.</p>'
            )
        
        flow = Flow.from_client_secrets_file(
            credentials_file, scopes=SCOPES, state=state,
            redirect_uri=flask.url_for('oauth2callback', _external=True)
        )
        authorization_response = flask.request.url
        flow.fetch_token(authorization_response=authorization_response)
        
        credentials = flow.credentials
        
        # Debug logging
        logger.info(f"Credentials received: token={bool(credentials.token)}, refresh_token={bool(credentials.refresh_token)}")
        
        # Đảm bảo tất cả các trường cần thiết có giá trị
        if not credentials.refresh_token:
            logger.error("No refresh token received from OAuth")
            # Xóa session và yêu cầu đăng nhập lại với consent
            flask.session.clear()
            return flask.render_template_string(
                '''<h1>Cần xác thực lại</h1>
                <p>Để ứng dụng hoạt động đúng, bạn cần cấp quyền truy cập offline.</p>
                <p><a href="/login">Đăng nhập lại và cấp quyền</a></p>
                <p><small>Lưu ý: Hãy đảm bảo chọn "Allow" cho tất cả các quyền được yêu cầu.</small></p>'''
            )
        
        flask.session['credentials'] = {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri or 'https://oauth2.googleapis.com/token',
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes
        }
        
        # Xóa state khỏi session sau khi sử dụng
        flask.session.pop('state', None)
        
        return flask.redirect(flask.url_for('index'))
    except Exception as e:
        logger.error(f"OAuth callback error: {e}")
        return flask.render_template_string(
            '<h1>Lỗi xác thực</h1><p>Không thể hoàn tất quá trình xác thực. Vui lòng <a href="/login">thử lại</a>.</p>'
        )

@app.route('/logout')
def logout():
    flask.session.pop('credentials', None)
    return flask.redirect(flask.url_for('index'))

@app.route('/force_reauth')
def force_reauth():
    """Buộc người dùng đăng nhập lại để lấy refresh_token mới."""
    flask.session.clear()
    return flask.redirect(flask.url_for('login'))

@app.route('/copy', methods=['GET'])
def copy():
    """Xử lý sao chép đơn lẻ và stream tiến trình về trình duyệt."""
    if 'credentials' not in flask.session:
        return flask.Response("Unauthorized", status=401)

    try:
        # Tạo credentials object với đầy đủ thông tin
        creds_data = flask.session['credentials']
        
        # Kiểm tra tính hợp lệ của credentials data
        required_fields = ['token', 'refresh_token', 'client_id', 'client_secret']
        missing_fields = [field for field in required_fields if not creds_data.get(field)]
        
        if missing_fields:
            logger.error(f"Missing credentials fields: {missing_fields}")
            if 'refresh_token' in missing_fields:
                # Trường hợp đặc biệt cho refresh_token
                return flask.Response(
                    "Refresh token is missing. Please <a href='/force_reauth'>re-authenticate</a> to grant offline access.",
                    status=401,
                    headers={'Content-Type': 'text/html'}
                )
            return flask.Response("Invalid credentials. Please login again.", status=401)
        
        try:
            creds = Credentials(
                token=creds_data.get('token'),
                refresh_token=creds_data.get('refresh_token'),
                token_uri=creds_data.get('token_uri', 'https://oauth2.googleapis.com/token'),
                client_id=creds_data.get('client_id'),
                client_secret=creds_data.get('client_secret'),
                scopes=creds_data.get('scopes')
            )
            
            # Kiểm tra và refresh token nếu cần
            if creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    # Cập nhật session với token mới
                    flask.session['credentials']['token'] = creds.token
                    logger.info("Credentials refreshed successfully")
                except Exception as refresh_error:
                    logger.error(f"Failed to refresh credentials: {refresh_error}")
                    return flask.Response("Failed to refresh credentials. Please login again.", status=401)
            
            service = build('drive', 'v3', credentials=creds)
        except Exception as creds_error:
            logger.error(f"Failed to create credentials: {creds_error}")
            return flask.Response("Invalid credentials format. Please login again.", status=401)
        
        source_url = flask.request.args.get('drive_url')
        destination_url = flask.request.args.get('destination_url')
        
        source_id = get_id_from_url(source_url)
        if not source_id:
            return flask.Response("Invalid source URL", status=400)

        destination_folder_id = None
        if destination_url:
            destination_folder_id = get_id_from_url(destination_url)
            if not destination_folder_id:
                return flask.Response("Invalid destination URL", status=400)

        def generate_progress():
            """Generator để stream dữ liệu về trình duyệt."""
            
            def send_event(data):
                """Gửi dữ liệu theo định dạng Server-Sent Events."""
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            copied_count = 0
            total_count = 0

            try:
                # Lấy thông tin item nguồn
                yield from send_event({'message': '🔍 Đang phân tích nguồn...', 'percent': 0})
                
                item_metadata = safe_file_operation(
                    service.files().get,
                    fileId=source_id, 
                    fields='mimeType, name, parents'
                ).execute()
                
                is_folder = item_metadata['mimeType'] == 'application/vnd.google-apps.folder'
                item_name = item_metadata.get('name', 'Unknown')

                # Đếm tổng số file
                yield from send_event({'message': f'📊 Đang đếm tổng số file trong "{item_name}"...', 'percent': 5})
                
                if is_folder:
                    total_count = count_files_recursively(service, source_id)
                else:
                    total_count = 1
                
                if total_count == 0 and is_folder:
                    yield from send_event({'message': '📂 Thư mục nguồn rỗng, không có gì để sao chép.', 'percent': 100})
                    yield from send_event({'message': '🎉 Hoàn thành!'})
                    return

                yield from send_event({
                    'message': f'✅ Tìm thấy {total_count} file(s) trong "{item_name}". Bắt đầu sao chép...',
                    'percent': 10
                })

                def copy_folder_recursively_stream(s_id, d_parent_id=None, current_path=""):
                    """Sao chép thư mục một cách đệ quy với streaming."""
                    nonlocal copied_count
                    
                    try:
                        # Lấy thông tin thư mục
                        folder_meta = safe_file_operation(
                            service.files().get,
                            fileId=s_id,
                            fields='name'
                        ).execute()
                        
                        folder_name = folder_meta.get('name', 'Unknown Folder')
                        new_path = f"{current_path}/{folder_name}" if current_path else folder_name
                        
                        yield from send_event({'message': f"📁 Tạo thư mục: {new_path}"})

                        # Tạo thư mục mới
                        new_folder = safe_file_operation(
                            service.files().create,
                            body={
                                'name': folder_name,
                                'mimeType': 'application/vnd.google-apps.folder',
                                'parents': [d_parent_id] if d_parent_id else []
                            },
                            fields='id'
                        ).execute()
                        
                        new_folder_id = new_folder.get('id')

                        # Lấy danh sách items trong thư mục
                        page_token = None
                        while True:
                            query = f"'{s_id}' in parents and trashed = false"
                            results = safe_file_operation(
                                service.files().list,
                                q=query,
                                pageSize=1000,
                                fields="nextPageToken, files(id, name, mimeType)",
                                pageToken=page_token
                            ).execute()
                            
                            items = results.get('files', [])
                            
                            for item in items:
                                if item['mimeType'] == 'application/vnd.google-apps.folder':
                                    # Đệ quy cho thư mục con
                                    yield from copy_folder_recursively_stream(
                                        item['id'], 
                                        new_folder_id, 
                                        new_path
                                    )
                                else:
                                    # Sao chép file
                                    file_name = item.get('name', 'Unknown File')
                                    file_path = f"{new_path}/{file_name}"
                                    
                                    yield from send_event({'message': f"📄 Sao chép: {file_path}"})
                                    
                                    safe_file_operation(
                                        service.files().copy,
                                        fileId=item['id'],
                                        body={
                                            'name': file_name,
                                            'parents': [new_folder_id]
                                        }
                                    ).execute()
                                    
                                    copied_count += 1
                                    percent = min(90, 10 + int((copied_count / total_count) * 80))
                                    yield from send_event({
                                        'percent': percent,
                                        'message': f"✔️ {file_name}"
                                    })
                                    
                                    # Nghỉ ngắn để tránh rate limit
                                    time.sleep(0.1)
                            
                            page_token = results.get('nextPageToken')
                            if not page_token:
                                break

                    except HttpError as error:
                        yield from send_event({
                            'message': f"❌ Lỗi xử lý thư mục {current_path}: {error}",
                            'isError': True
                        })

                # Bắt đầu quá trình sao chép
                if is_folder:
                    yield from copy_folder_recursively_stream(source_id, destination_folder_id)
                else:
                    # Sao chép file đơn lẻ
                    yield from send_event({'message': f"📄 Đang sao chép file: {item_name}...", 'percent': 50})
                    
                    safe_file_operation(
                        service.files().copy,
                        fileId=source_id,
                        body={
                            'name': item_name,
                            'parents': [destination_folder_id] if destination_folder_id else []
                        }
                    ).execute()
                    
                    copied_count += 1
                    yield from send_event({'percent': 90, 'message': f"✔️ {item_name}"})
                
                yield from send_event({'message': '🎉 Hoàn thành!', 'percent': 100})

            except HttpError as error:
                error_msg = f"❌ Lỗi Google Drive API: {error}"
                if "File not found" in str(error):
                    error_msg = "❌ Không tìm thấy file/thư mục nguồn. Kiểm tra link và quyền truy cập."
                elif "Insufficient Permission" in str(error):
                    error_msg = "❌ Không có quyền truy cập. Đảm bảo file/thư mục được chia sẻ công khai hoặc với tài khoản của bạn."
                
                yield from send_event({'message': error_msg, 'isError': True})
            except Exception as error:
                yield from send_event({
                    'message': f"❌ Lỗi không xác định: {str(error)}",
                    'isError': True
                })

        return flask.Response(
            generate_progress(), 
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no'  # Cho nginx
            }
        )
        
    except Exception as e:
        logger.error(f"Copy error: {e}")
        return flask.Response(f"Server error: {str(e)}", status=500)

@app.route('/batch_copy', methods=['POST'])
def batch_copy():
    """Xử lý sao chép hàng loạt từ file Excel."""
    if 'credentials' not in flask.session:
        return flask.Response("Unauthorized", status=401)

    if 'excel_file' not in flask.request.files:
        return flask.Response("No file uploaded", status=400)

    file = flask.request.files['excel_file']
    if file.filename == '' or not allowed_file(file.filename):
        return flask.Response("Invalid file", status=400)

    try:
        # Tạo credentials object với đầy đủ thông tin
        creds_data = flask.session['credentials']
        
        # Kiểm tra tính hợp lệ của credentials data
        required_fields = ['token', 'refresh_token', 'client_id', 'client_secret']
        missing_fields = [field for field in required_fields if not creds_data.get(field)]
        
        if missing_fields:
            logger.error(f"Missing credentials fields: {missing_fields}")
            if 'refresh_token' in missing_fields:
                # Trường hợp đặc biệt cho refresh_token
                return flask.Response(
                    "Refresh token is missing. Please <a href='/force_reauth'>re-authenticate</a> to grant offline access.",
                    status=401,
                    headers={'Content-Type': 'text/html'}
                )
            return flask.Response("Invalid credentials. Please login again.", status=401)
        
        try:
            creds = Credentials(
                token=creds_data.get('token'),
                refresh_token=creds_data.get('refresh_token'),
                token_uri=creds_data.get('token_uri', 'https://oauth2.googleapis.com/token'),
                client_id=creds_data.get('client_id'),
                client_secret=creds_data.get('client_secret'),
                scopes=creds_data.get('scopes')
            )
            
            # Kiểm tra và refresh token nếu cần
            if creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    # Cập nhật session với token mới
                    flask.session['credentials']['token'] = creds.token
                    logger.info("Credentials refreshed successfully")
                except Exception as refresh_error:
                    logger.error(f"Failed to refresh credentials: {refresh_error}")
                    return flask.Response("Failed to refresh credentials. Please login again.", status=401)
            
            service = build('drive', 'v3', credentials=creds)
        except Exception as creds_error:
            logger.error(f"Failed to create credentials: {creds_error}")
            return flask.Response("Invalid credentials format. Please login again.", status=401)
        
        # Lưu file upload
        filename = secure_filename(file.filename)
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(file_path)

        def generate_batch_progress():
            """Generator cho batch copy process."""
            
            def send_event(data):
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                # Đọc file Excel
                yield from send_event({'message': '📊 Đang đọc file Excel...', 'percent': 0})
                url_pairs = read_excel_file(file_path)
                
                if not url_pairs:
                    yield from send_event({'message': '❌ File Excel rỗng hoặc không có dữ liệu hợp lệ.', 'isError': True})
                    return
                
                total_items = len(url_pairs)
                yield from send_event({
                    'message': f'✅ Tìm thấy {total_items} mục để sao chép.',
                    'percent': 5
                })

                success_count = 0
                error_count = 0

                for index, (source_url, dest_url) in enumerate(url_pairs, 1):
                    try:
                        yield from send_event({
                            'message': f'🔄 [{index}/{total_items}] Đang xử lý: {source_url[:50]}...',
                            'percent': 5 + int((index - 1) / total_items * 90)
                        })

                        source_id = get_id_from_url(source_url)
                        if not source_id:
                            raise ValueError(f"URL không hợp lệ: {source_url}")

                        dest_folder_id = None
                        if dest_url:
                            dest_folder_id = get_id_from_url(dest_url)
                            if not dest_folder_id:
                                raise ValueError(f"URL đích không hợp lệ: {dest_url}")

                        # Lấy thông tin file/folder nguồn
                        item_metadata = safe_file_operation(
                            service.files().get,
                            fileId=source_id,
                            fields='mimeType, name'
                        ).execute()
                        
                        item_name = item_metadata.get('name', 'Unknown')
                        is_folder = item_metadata['mimeType'] == 'application/vnd.google-apps.folder'

                        if is_folder:
                            # Sao chép thư mục (đơn giản hóa cho batch)
                            yield from send_event({'message': f'📁 Sao chép thư mục: {item_name}'})
                            
                            # Tạo thư mục gốc
                            new_folder = safe_file_operation(
                                service.files().create,
                                body={
                                    'name': item_name,
                                    'mimeType': 'application/vnd.google-apps.folder',
                                    'parents': [dest_folder_id] if dest_folder_id else []
                                },
                                fields='id'
                            ).execute()
                            
                            # Sao chép nội dung thư mục (chỉ level đầu tiên để tránh quá phức tạp)
                            query = f"'{source_id}' in parents and trashed = false"
                            results = safe_file_operation(
                                service.files().list,
                                q=query,
                                pageSize=100,
                                fields="files(id, name, mimeType)"
                            ).execute()
                            
                            items = results.get('files', [])
                            for item in items:
                                if item['mimeType'] != 'application/vnd.google-apps.folder':
                                    safe_file_operation(
                                        service.files().copy,
                                        fileId=item['id'],
                                        body={
                                            'name': item['name'],
                                            'parents': [new_folder.get('id')]
                                        }
                                    ).execute()
                        else:
                            # Sao chép file đơn lẻ
                            yield from send_event({'message': f'📄 Sao chép file: {item_name}'})
                            safe_file_operation(
                                service.files().copy,
                                fileId=source_id,
                                body={
                                    'name': item_name,
                                    'parents': [dest_folder_id] if dest_folder_id else []
                                }
                            ).execute()

                        success_count += 1
                        yield from send_event({'message': f'✔️ [{index}/{total_items}] Hoàn thành: {item_name}'})
                        
                        # Nghỉ ngắn giữa các lần sao chép
                        time.sleep(0.5)

                    except Exception as item_error:
                        error_count += 1
                        yield from send_event({
                            'message': f'❌ [{index}/{total_items}] Lỗi: {str(item_error)}',
                            'isError': True
                        })
                
                # Tổng kết
                final_percent = 100
                summary_msg = f'🎉 Hoàn thành! Thành công: {success_count}/{total_items}'
                if error_count > 0:
                    summary_msg += f', Lỗi: {error_count}'
                
                yield from send_event({
                    'message': summary_msg,
                    'percent': final_percent
                })

            except Exception as error:
                yield from send_event({
                    'message': f'❌ Lỗi đọc file Excel: {str(error)}',
                    'isError': True
                })
            finally:
                # Xóa file tạm
                try:
                    os.remove(file_path)
                except:
                    pass

        return flask.Response(
            generate_batch_progress(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no'
            }
        )

    except Exception as e:
        logger.error(f"Batch copy error: {e}")
        return flask.Response(f"Server error: {str(e)}", status=500)

if __name__ == '__main__':
    app.run(debug=True)