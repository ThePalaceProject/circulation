from pydantic import EmailStr, PositiveInt

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class EmailConfiguration(ServiceConfiguration):
    class Config:
        env_prefix = "PALACE_MAIL_"

    server: str | None = None
    port: PositiveInt = 25
    username: str | None = None
    password: str | None = None
    sender: EmailStr | None = None
