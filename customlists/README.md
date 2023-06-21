# customlists

In order to copy _customlists_ between CM instances, a suite of three command-line tools are provided.

* `bin/customlist_export` exports the contents of custom lists from an existing CM to a local file.
* `bin/customlist_import` imports customlists from a local file and creates/populates lists on a target CM. The tool
  produces a report in a machine-readable format that describes the work performed.
* `bin/customlist_explain` takes a machine-readable report produced by
  `bin/customlist_import` and produces a CSV file containing any errors that were encountered along with suggestions for
  manual remediation.

## Synopsis

```bash
$ ./bin/customlist_export --help
usage: customlist_export [-h] [--schema-file SCHEMA_FILE]
--server SERVER --username USERNAME --password PASSWORD --output OUTPUT
 --library-name LIBRARY_NAME [--list LIST [LIST ...]] [--verbose]

Fetch one or more custom lists.

optional arguments:
  -h, --help            show this help message and exit
  --schema-file SCHEMA_FILE
                        The path to customlists.schema.json
  --server SERVER       The address of the CM
  --username USERNAME   The CM admin username
  --password PASSWORD   The CM admin password
  --output OUTPUT       The output file
  --library-name LIBRARY_NAME
                        The short name of the library that owns the lists.
  --list LIST [LIST ...]
                        Only export the named list (may be repeated)
  --verbose, -v         Increase verbosity (can be specified multiple times to export multiple lists)

```

```bash
$ ./bin/customlist_import --help
usage: customlist_import [-h] --server SERVER --username USERNAME --password PASSWORD
[--schema-file SCHEMA_FILE] [--schema-report-file SCHEMA_REPORT_FILE]
--library-name LIBRARY_NAME --file FILE --output OUTPUT [--dry-run] [--verbose]

Import custom lists.

optional arguments:
  -h, --help            show this help message and exit
  --server SERVER       The address of the CM
  --username USERNAME   The CM admin username
  --password PASSWORD   The CM admin password
  --schema-file SCHEMA_FILE
                        The schema file for custom lists
  --schema-report-file SCHEMA_REPORT_FILE
                        The schema file for custom list reports
  --library-name LIBRARY_NAME
                        The destination library short name
  --file FILE           The customlists file
  --output OUTPUT       The output report
  --dry-run             Show what would be done, but don't do it.
  --verbose, -v         Increase verbosity (can be specified multiple times)


```

```bash
$ ./bin/customlist_explain --help
usage: customlist_explain [-h] [--verbose] [--report-schema-file REPORT_SCHEMA_FILE]
--report-file REPORT_FILE --output-csv-file OUTPUT_CSV_FILE

Explain what went wrong during an import.

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v         Increase verbosity
  --report-schema-file REPORT_SCHEMA_FILE
                        The schema file for custom list reports
  --report-file REPORT_FILE
                        The report file that was produced during importing
  --output-csv-file OUTPUT_CSV_FILE
                        The output CSV file containing the list of books to be fixed

```

## Example

The following copies all lists that belong to the library with the short name `HAZELNUT` from `source.example.com`
to the library `WALNUT` on `target.example.com`, producing an `output.csv` file that  contains any manual steps that
might need to be performed afterwards:

```bash
$ ./bin/customlist_export \
  --server http://source.example.com \
  --username admin-example \
  --password 12345678 \
  --library-name HAZELNUT \
  --output export.json

$ ./bin/customlist_import \
  --server http://target.example.com \
  --username admin-example \
  --password 12345678 \
  --library-name WALNUT \
  --file export.json \
  --output report.json

$ ./bin/customlist_explain \
  --report-file report.json \
  --output-csv-file output.csv
```
