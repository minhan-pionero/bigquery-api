# Japanese Email Template for Platform Errors

このドキュメントでは、LinkedInとFacebookのプラットフォームエラー用の日本語メールテンプレートの使用方法を説明します。

## API Endpoint

```
POST /email/error-report
```

## Request Format

シンプルな3つのパラメータのみで日本語エラーメールを送信できます：

```json
{
    "platform": "LinkedIn",  // または "Facebook"
    "method": "クローラー",    // または "友達取得"
    "error": "エラーメッセージをここに記述"
}
```

### Email Template Output

メールは以下の形式で送信されます：

**件名：** `{platform}エラー通知`

**本文：**
```
{platform}の{method}でエラーが発生しました

エラー内容
========================
{error}
========================
```

## Supported Platforms and Methods

### LinkedIn
- **クローラー**: プロフィール情報の収集
- **友達取得**: 接続情報の取得

### Facebook  
- **クローラー**: プロフィール情報の収集
- **友達取得**: 友達情報の取得

## Example Requests

### LinkedIn クローラーエラー
```bash
curl -X POST "http://localhost:8000/email/error-report" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "LinkedIn",
    "method": "クローラー",
    "error": "接続がタイムアウトしました。ネットワーク設定を確認してください。"
  }'
```

### Facebook 友達取得エラー
```bash
curl -X POST "http://localhost:8000/email/error-report" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "Facebook",
    "method": "友達取得", 
    "error": "友達データの取得に失敗しました。APIキーを確認してください。"
  }'
```

## Testing

テストスクリプトを実行してAPIをテストできます：

```bash
python test_japanese_email.py
```

## Notes

- 3つのパラメータのみ必要：`platform`, `method`, `error`
- プラットフォームはLinkedInまたはFacebookのみサポート
- 自動的に日本語テンプレートでメール送信されます