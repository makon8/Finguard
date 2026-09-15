from pyspark import pipelines as dp
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_fraud_alert_email(to_email, alert_data):
    """
    Send fraud card alert email to customer
    """
    sender_email = "maksymowicz.konrad@gmail.com"
    try:
        gmail_api_key = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")
    except Exception:
        gmail_api_key = 'crgu czme pbwp mdbd' 
    
    body = f"""
    <html>
    <body>
    
    <h2 style="color: #d9534f;">⚠️ FRAUD ALERT - Immediate Action Required</h2>
    
    <p>Dear {alert_data.get('customer_name', 'Valued Customer')},</p>
    
    <p><b style="color: #d9534f;">We detected a potentially fraudulent transaction on your card:</b></p>
    
    <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Alert ID:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('alert_id', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Alert Type:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;"><b style="color: #d9534f;">{alert_data.get('alert_type', 'N/A')}</b></td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Risk Level:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;"><b style="color: #d9534f;">{alert_data.get('risk_level', 'N/A')}</b></td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Card Number:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('card_number', 'N/A')}</td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Transaction Amount:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>{alert_data.get('amount', 0)} {alert_data.get('currency', 'USD')}</b></td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Merchant:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('merchant_name', 'N/A')}</td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Merchant Category:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('merchant_category', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Transaction Location:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('transaction_city', 'N/A')}, {alert_data.get('transaction_country', 'N/A')}</td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Transaction Time:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('transaction_timestamp', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Payment Channel:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('payment_channel', 'N/A')}</td>
        </tr>
    </table>
    
    <h3 style="color: #d9534f;">Fraud Detection Details:</h3>
    <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
        <tr style="background-color: #fff3cd;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Watchlist ID:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('watchlist_id', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Watch Type:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('watch_type', 'N/A')}</td>
        </tr>
        <tr style="background-color: #fff3cd;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Reason:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('reason_description', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Recommended Action:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>{alert_data.get('action', 'N/A')}</b></td>
        </tr>
        <tr style="background-color: #fff3cd;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Watchlist Location:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('watchlist_city', 'N/A')}, {alert_data.get('watchlist_country', 'N/A')}</td>
        </tr>
    </table>
    
    <p style="background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; border-radius: 5px;">
        <b>⚠️ URGENT ACTION REQUIRED:</b><br>
        If you did NOT authorize this transaction, please contact us immediately at our fraud hotline or reply to this email.
        Your card may be temporarily blocked for your protection.
    </p>
    
    <p>
    Thanks,<br>
    <b>FinGuard Security Team</b><br>
    <i>Protecting your financial security 24/7</i>
    </p>
    
    </body>
    </html>
    """
    
    subject = f"🚨 FRAUD ALERT - {alert_data.get('alert_type', 'Suspicious Activity')} - {alert_data.get('transaction_id', 'N/A')}"
    
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, gmail_api_key)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Error sending fraud alert email to {to_email}: {e}")
        return False


@dp.foreach_batch_sink(name="fraud_email_alert_sink")
def fraud_email_alert_sink(df, batch_id):
    """
    Process each micro-batch of fraud alerts and send emails to customers
    """
    alerts = df.collect()
    
    print(f"Processing fraud alert batch {batch_id} with {len(alerts)} alerts")
    
    success_count = 0
    failure_count = 0
    
    for alert in alerts:
        alert_data = {
            'alert_id': alert.alert_id,
            'alert_type': alert.alert_type,
            'transaction_id': alert.transaction_id,
            'customer_name': alert.customer_name,
            'customer_email': alert.customer_email,
            'card_number': alert.card_number,
            'amount': alert.amount,
            'currency': alert.currency,
            'merchant_name': alert.merchant_name,
            'merchant_category': alert.merchant_category,
            'transaction_city': alert.transaction_city,
            'transaction_country': alert.transaction_country,
            'transaction_timestamp': str(alert.transaction_timestamp),
            'payment_channel': alert.payment_channel,
            'watchlist_id': alert.watchlist_id,
            'watch_type': alert.watch_type,
            'risk_level': alert.risk_level,
            'action': alert.action,
            'reason_description': alert.reason_description,
            'watchlist_city': alert.watchlist_city,
            'watchlist_country': alert.watchlist_country
        }
        
        if send_fraud_alert_email(alert.customer_email, alert_data):
            success_count += 1
            print(f"Sent fraud alert {alert.alert_id} to {alert.customer_email}")
        else:
            failure_count += 1
    
    print(f"Fraud alert batch {batch_id} complete: {success_count} sent, {failure_count} failed")


@dp.append_flow(
    target="fraud_email_alert_sink",
    name="fraud_email_notifications_flow",
    comment="Routes fraud card alerts to email notification sink for real-time fraud notifications"
)
def fraud_email_notifications_flow():
    """
    Stream fraud card alerts to email sink for real-time fraud notifications
    """
    return spark.readStream.table("finguard2.gold.fraud_card_alert")