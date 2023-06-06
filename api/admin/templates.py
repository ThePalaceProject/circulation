admin = """
<!doctype html>
<html>
<head>
<title>{{ app_name }}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="{{ admin_css }}" rel="stylesheet" />
</head>
<body>
  <script src="{{ admin_js }}"></script>
  <script>
    var circulationAdmin = new CirculationAdmin({
        csrfToken: "{{ csrf_token }}",
        tos_link_href: "{{ sitewide_tos_href }}",
        tos_link_text: "{{ sitewide_tos_text }}",
        showCircEventsDownload: {{ "true" if show_circ_events_download else "false" }},
        settingUp: {{ "true" if setting_up else "false" }},
        email: "{{ email }}",
        roles: [{% for role in roles %}{"role": "{{role.role}}"{% if role.library %}, "library": "{{role.library.short_name}}"{% endif %} },{% endfor %}],
        featureFlags: {
          enableAutoList: true,
        },
    });
  </script>
</body>
</html>
"""

admin_sign_in_again = """
<!doctype html>
<html>
<head><title>{{ app_name }}</title></head>
<body>
  <p>You are now logged in. You may close this window and try your request again.
</body>
</html>
"""

response_template_with_message_and_redirect_button = """<!DOCTYPE HTML>
<html lang="en">
{head_html}
<body style="%(body_style)s">
<p>%(message)s</p>
<hr style="{hr}">
<a href="%(redirect_link)s" style="{link}">%(button_text)s</a>
</body>
</html>
"""

sign_in_template = """
<form action="%(password_sign_in_url)s" method="post">
<input type="hidden" name="redirect" value="%(redirect)s"/>
<label style="{label}">Email <input type="text" name="email" style="{input}" /></label>
<label style="{label}">Password <input type="password" name="password" style="{input}" /></label>
<a href="%(forgot_password_url)s">Forgot password?</a>
<button type="submit" style="{button}">Sign In</button>
</form>
"""

forgot_password_template = """
<form action="%(forgot_password_url)s" method="post">
<input type="hidden" name="redirect" value="%(redirect)s"/>
<label style="{label}">Email <input type="email" name="email" style="{input}" required/></label>
<button type="submit" style="{button}">Send reset password email</button>
</form>
"""

reset_password_template = """
<form action="%(reset_password_url)s" method="post">
<input type="hidden" name="redirect" value="%(redirect)s"/>
<label style="{label}">New Password <input type="password" name="password" style="{input}" required/></label>
<label style="{label}">Confirm New Password <input type="password" name="confirm_password" style="{input}" required/></label>
<button type="submit" style="{button}">Submit</button>
</form>
"""

reset_password_email_text = """
Hello,

You are receiving this email because you requested a password reset for your account at the {{ app_name }}.

To reset your password paste the following link in your browser's address bar: {{ reset_password_url }}

If you have not requested a password reset please contact someone from the The Palace Project team.

Thank you,
The Palace Project team
"""


reset_password_email_html = """
<p>Hello,</p>

<p>You are receiving this email because you requested a password reset for your account at the {{ app_name }}.</p>

<p>
    To reset your password
    <a href="{{ reset_password_url }}">click here</a>.
</p>

<p>
    Alternatively, you can paste the following link in your browser's address bar: <br>
    {{ reset_password_url }}
</p>

<p>If you have not requested a password reset please contact someone from the The Palace Project team.</p>

<p>
    Thank you, <br>
    The Palace Project team
</p>
"""
