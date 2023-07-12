import os
import subprocess
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders



def sendmail(
        email_from:str,
        email_to:list,
        email_subject:str,
        email_body:str,
        smtp_server:str,
        smtp_port:int,
        smtp_username:str,
        smtp_password:str,
        attachment_path:str=None,
        body_subset:str='plain'):
    # Create the email message object
    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = COMMASPACE.join(email_to)
    msg['Subject'] = email_subject

    # Add the body text to the email
    msg.attach(MIMEText(email_body,body_subset))

    # Add the attachment to the email
    if attachment_path: 
        with open(attachment_path, 'rb') as f:
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(f.read())
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', 
                                  f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(attachment)

    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(smtp_username, smtp_password)
        smtp.sendmail(email_from, email_to, msg.as_string())

# Return the git revision as a string
def git_version():
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.Popen(cmd, stdout = subprocess.PIPE, env=env).communicate()[0]
        return out

    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', 'HEAD'])
        GIT_REVISION = out.strip().decode('ascii')
    except OSError:
        GIT_REVISION = "Unknown"

    return GIT_REVISION

def get_day_of_week():
    import datetime
    return datetime.datetime.today().weekday()