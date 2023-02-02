import os

from redmail import EmailSender


class EmailManager:
    MAIL_SERVER = os.environ.get("SIMPLIFIED_MAIL_SERVER")
    MAIL_PORT = int(os.environ.get("SIMPLIFIED_MAIL_PORT", "25"))
    MAIL_USERNAME = os.environ.get("SIMPLIFIED_MAIL_USERNAME")
    MAIL_PASSWORD = os.environ.get("SIMPLIFIED_MAIL_PASSWORD")

    @classmethod
    def send_email(cls, subject, sender, receivers, text=None, html=None):
        email_sender = EmailSender(
            host=cls.MAIL_SERVER,
            port=cls.MAIL_PORT,
            username=cls.MAIL_USERNAME,
            password=cls.MAIL_PASSWORD,
        )

        email_sender.send(
            subject=subject, sender=sender, receivers=receivers, text=text, html=html
        )
