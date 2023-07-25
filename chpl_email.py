import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders


def send_email(
    smtp_username,
    smtp_password,
    subject, 
    message, 
    from_addr, 
    to_addr,
    files
    # cc_addr=None, 
    # bcc_addr=None, 
):
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = COMMASPACE.join(to_addr)
    # msg['Cc'] = COMMASPACE.join(cc_addr)
    # msg['Bcc'] = COMMASPACE.join(bcc_addr)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=file)  
        msg.attach(part)
        
    smtp = smtplib.SMTP('smtp.mandrillapp.com', 587)
    smtp.login(smtp_username, smtp_password)
    smtp.send_message(msg)
    smtp.close()
    
    
# use like this:
# send_email(
#     subject="Here's your data",
#     message="Please find attached the data you requested.",
#     from_addr="sender@example.com",
#     to_addr=["recipient1@example.com", "recipient2@example.com"],
#     files=["test.csv"],
# )