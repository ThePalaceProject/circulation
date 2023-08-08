# class RegistrationConstants:
#     """Constants used for library registration."""
#
#     # A library registration attempt may succeed or fail.
#     LIBRARY_REGISTRATION_STATUS = "library-registration-status"
#     SUCCESS_STATUS = "success"
#     FAILURE_STATUS = "failure"
#
#     # A library may be registered in a 'testing' stage or a
#     # 'production' stage. This represents the _library's_ opinion
#     # about whether the integration is ready for production. The
#     # library won't actually be in production (whatever that means for
#     # a given integration) until the _remote_ also thinks it should.
#     LIBRARY_REGISTRATION_STAGE = "library-registration-stage"
#     TESTING_STAGE = "testing"
#     PRODUCTION_STAGE = "production"
#     VALID_REGISTRATION_STAGES = [TESTING_STAGE, PRODUCTION_STAGE]
#
#     # A registry may provide access to a web client. If so, we'll store
#     # the URL so we can enable CORS headers in requests from that client,
#     # and use it in MARC records so the library's main catalog can link
#     # to it.
#     LIBRARY_REGISTRATION_WEB_CLIENT = "library-registration-web-client"
