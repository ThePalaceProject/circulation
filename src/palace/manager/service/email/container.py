from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider
from redmail.email.sender import EmailSender

from palace.manager.service.email.email import (
    SendEmailCallable,
    emailer_factory,
    send_email,
)


class Email(DeclarativeContainer):
    config = providers.Configuration()

    emailer: Provider[EmailSender] = providers.Singleton(
        emailer_factory,
        host=config.server,
        port=config.port,
        username=config.username,
        password=config.password,
    )

    send_email: SendEmailCallable = providers.Callable(
        send_email,
        emailer=emailer,
        sender=config.sender,
    )
