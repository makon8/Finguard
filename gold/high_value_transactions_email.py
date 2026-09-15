from pyspark import pipelines as dp
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_alert_email(to_email, alert_data):
    """
    Send high-value transaction alert email to customer
    """
    sender_email = "maksymowicz.konrad@gmail.com"
    try:
        gmail_api_key = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")
    except Exception:
        gmail_api_key = 'crgu czme pbwp mdbd' 
    
    body = f"""
    <html>
    <body>
    
    <h2>High-Value Transaction Alert</h2>
    
    <p>Dear {alert_data.get('customer_name', 'Valued Customer')},</p>
    
    <p>We detected a transaction that exceeds your set transaction limit:</p>
    
    <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Alert ID:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('alert_id', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Transaction ID:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('transaction_id', 'N/A')}</td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Transaction Amount:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>{alert_data.get('transaction_amount', 0)} {alert_data.get('currency', 'USD')}</b></td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Your Transaction Limit:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('transaction_limit', 0)} {alert_data.get('currency', 'USD')}</td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Merchant:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('merchant_name', 'N/A')}</td>
        </tr>
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Location:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('city', 'N/A')}, {alert_data.get('country', 'N/A')}</td>
        </tr>
        <tr style="background-color: #f2f2f2;">
            <td style="border: 1px solid #ddd; padding: 8px;"><b>Transaction Time:</b></td>
            <td style="border: 1px solid #ddd; padding: 8px;">{alert_data.get('transaction_timestamp', 'N/A')}</td>
        </tr>
    </table>
    
    <p><b>If this transaction was not authorized by you, please contact us immediately.</b></p>
    
    <p>
    Thanks,<br>
    <b>FinGuard Support Team</b><br>
    <i>Protecting your financial security 24/7</i>
    </p>
    
    </body>
    </html>
    """
    
    subject = f"High-Value Transaction Alert - {alert_data.get('transaction_id', 'N/A')}"
    
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
        print(f"Error sending email to {to_email}: {e}")
        return False


@dp.foreach_batch_sink(name="email_alert_sink")
def email_alert_sink(df, batch_id):
    """
    Process each micro-batch of alerts and send emails to customers
    """
    alerts = df.collect()
    
    print(f"Processing batch {batch_id} with {len(alerts)} alerts")
    
    success_count = 0
    failure_count = 0
    
    for alert in alerts:
        alert_data = {
            'alert_id': alert.alert_id,
            'transaction_id': alert.transaction_id,
            'customer_name': alert.customer_name,
            'customer_email': alert.customer_email,
            'transaction_amount': alert.transaction_amount,
            'transaction_limit': alert.transaction_limit,
            'currency': alert.currency,
            'merchant_name': alert.merchant_name,
            'city': alert.city,
            'country': alert.country,
            'transaction_timestamp': str(alert.transaction_timestamp)
        }
        
        if send_alert_email(alert.customer_email, alert_data):
            success_count += 1
            print(f"Sent alert {alert.alert_id} to {alert.customer_email}")
        else:
            failure_count += 1
    
    print(f"Batch {batch_id} complete: {success_count} sent, {failure_count} failed")


@dp.append_flow(
    target="email_alert_sink",
    name="email_notifications_flow",
    comment="Routes high-value transaction alerts to email notification sink"
)
def email_notifications_flow():
    """
    Stream high-value transaction alerts to email sink for real-time notifications
    """
    return spark.readStream.table("finguard2.gold.transactions")