from pydantic import EmailStr, PositiveInt
from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class EmailConfiguration(ServiceConfiguration):
    model_config = SettingsConfigDict(env_prefix="PALACE_MAIL_")

    server: str | None = None
    port: PositiveInt = 25
    username: str | None = None
    password: str | None = None
    sender: EmailStr | None = None
