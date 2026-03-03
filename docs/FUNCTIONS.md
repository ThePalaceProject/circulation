# Patron Blocking Rules — Allowed Functions

Patron blocking rule expressions are evaluated by a locked-down
[simpleeval](https://github.com/danthedeckie/simpleeval) sandbox.
Only the functions listed below may be called inside a rule expression.
Any reference to an unlisted function causes the rule to **fail closed**
(the patron is blocked at runtime; the rule is rejected at admin-save time).

---

## `age_in_years`

Calculates the age of a person in **whole years** from a date string.
Use this to write rules that gate access by age (e.g. block minors or
enforce senior-only services).

### Signature

```text
age_in_years(date_str, fmt=None) -> int
```

### Parameters

| Parameter  | Type            | Required |
|------------|-----------------|----------|
| `date_str` | `str`           | Yes      |
| `fmt`      | `str` or `None` | No       |

- **`date_str`** — A date string representing the person's date of birth.
  ISO 8601 format (`YYYY-MM-DD`) is tried first; if that fails,
  `dateutil.parser` is used as a fallback, accepting most common
  human-readable formats (e.g. `"Jan 1, 1990"`, `"01/01/1990"`).
- **`fmt`** — An explicit
  [`strptime`](https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime)
  format string (e.g. `"%d/%m/%Y"`). When supplied, no automatic parsing
  is attempted.

### Returns

`int` — The person's age in complete years (fractional years are truncated,
not rounded).

### Raises

`ValueError` — If `date_str` cannot be parsed (either by ISO 8601, the
supplied `fmt`, or `dateutil`). At runtime this causes the rule to
**fail closed**.

### Examples

```python
# Block patrons under 18 (field returned verbatim from the SIP2 server)
age_in_years({polaris_patron_birthdate}) < 18

# Block patrons under 18 using an explicit strptime format
age_in_years({dob_field}, "%d/%m/%Y") < 18

# Block patrons aged 65 or over (e.g. senior-only restriction)
age_in_years({polaris_patron_birthdate}) >= 65
```

---

## `int`

Converts a value to a Python `int`. Useful when the SIP2 server returns
a numeric field as a string (a common occurrence) and you need to compare
it numerically rather than lexicographically.

### Signature

```text
int(value) -> int
```

### Parameters

| Parameter | Type  | Required |
|-----------|-------|----------|
| `value`   | `Any` | Yes      |

- **`value`** — The value to convert. Typically a string such as `"3"` or
  a float such as `2.9`. Any value accepted by Python's built-in `int()` is
  valid. Passing a non-numeric string (e.g. `"adult"`) raises a `ValueError`
  and causes the rule to **fail closed**.

### Returns

`int` — The integer representation of `value`. Floating-point values are
**truncated** toward zero (e.g. `int("2.9")` raises `ValueError`; pass a
float literal or cast via `{field} * 1` first if you need truncation of
floats).

### Raises

`ValueError` — If `value` cannot be converted to an integer. At runtime
this causes the rule to **fail closed**.

### Examples

```python
# Block patron class codes above 2 (SIP2 returns the code as a string)
int({sipserver_patron_class}) > 2

# Block if a numeric expiry-year field indicates an expired account
int({expire_year}) < 2025
```

---

## Notes

- **String methods are available** — methods on Python `str` values can be
  called directly on string-valued placeholders. For example, to check
  whether a patron identifier starts with a certain prefix:

  ```python
  {patron_identifier}.startswith("1234")
  ```

- **Fail-closed behaviour** — any function call that raises an exception
  (e.g. an unparseable date or a non-numeric string passed to `int()`)
  causes the patron to be **blocked** at runtime and the rule to be
  **rejected** at admin-save time. Write test rules carefully using
  representative patron data before enabling them in production.
- **No other builtins** — Python builtins such as `len`, `str`, `float`,
  `abs`, and `round` are **not** available. If you need additional
  functions, request them via the standard feature-request process so they
  can be reviewed and added to `DEFAULT_ALLOWED_FUNCTIONS` in
  `rule_engine.py`.
- **Placeholder syntax** — field values from the SIP2 response are
  referenced as `{field_name}`. All fields returned by the SIP2
  `patron_information` command are available, plus the normalised `{fines}`
  key (a `float` derived from `fee_amount`).
