# Cách chạy chương trình (Windows)

## Cách đơn giản nhất

1. **Chạy một lệnh duy nhất:**
   ```cmd
   quick_start.bat
   ```
   Script này sẽ tự động:
   - Cài đặt môi trường Python
   - Cài đặt các thư viện cần thiết
   - Khởi động server

2. **Hoặc chạy từng bước:**
   ```cmd
   # Bước 1: Cài đặt môi trường
   setup.bat
   
   # Bước 2: Khởi động server
   start.bat
   ```

## Kết quả

Sau khi chạy thành công, bạn sẽ thấy:
- Server chạy tại: http://localhost:8000
- API docs tại: http://localhost:8000/docs
- Health check: http://localhost:8000/health

## Dừng chương trình

Nhấn `Ctrl + C` trong terminal để dừng server.

## Yêu cầu hệ thống

- Python 3.8 trở lên
- Kết nối internet (để tải thư viện)

## Troubleshooting

**Lỗi "Python không tìm thấy":**
- Cài đặt Python từ python.org
- Thêm Python vào PATH

**Lỗi "Permission denied":**
- Chạy Command Prompt với quyền Administrator
- Kiểm tra antivirus settings

**Server không khởi động:**
- Kiểm tra port 8000 có bị chiếm dụng không
- Kiểm tra Windows Firewall settings