
## Adding seeds

```bash
./target/scala-2.11/moca-0.0.1 -p 1732 -S 127.0.0.1:1731 -s seeds.txt
```


### Seeds file example

```
!& default
!= max-depth 3
!= robots-txt
!= same-domain
!= a-href

http://site0.test:8000
```


## Checking results for a specific seed

```bash
./target/scala-2.11/moca-0.0.1 -p 1732 -S 127.0.0.1:1731 -r 1
```


## Set Up PostgreSQL for `PgMapDBWorkRepo`

Just run the following on your console:

```shell
createdb moca
```


## Set Up PostgreSQL for `akka-persistence-sql-async`

Run the following on your console:

```bash
createdb akka-persistence
createuser --pwprompt akka-persistence
psql -h localhost akka-persistence
```

Followed by this on psql:

```sql
CREATE TABLE IF NOT EXISTS journal (
  persistence_id VARCHAR(255) NOT NULL,
  sequence_nr BIGINT NOT NULL,
  message BYTEA NOT NULL,
  PRIMARY KEY (persistence_id, sequence_nr)
);

CREATE TABLE IF NOT EXISTS snapshot (
  persistence_id VARCHAR(255) NOT NULL,
  sequence_nr BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  snapshot BYTEA NOT NULL,
  PRIMARY KEY (persistence_id, sequence_nr)
);

GRANT ALL PRIVILEGES ON database "akka-persistence" to "akka-persistence";
GRANT ALL PRIVILEGES ON TABLE "journal" to "akka-persistence";
GRANT ALL PRIVILEGES ON TABLE "snapshot" to "akka-persistence";
```

## Set Up FakeS3 for `S3ContentRepo`

1. Install rbenv (used [this](https://gorails.com/setup/osx/10.10-yosemite) as reference)
2. Set endpoint to `http://localhost:4568` in `main.conf`
3. Run:

```bash
gem install fakes3
fakes3 -r tmp-dir -p 4568
```


## TODO

* Re-enable Javascript execution on the JavaFX WebKit browser:
  	- it was removed due to issues with the jvm. Getting objects from it seem to make the whole process crash
  	- try by using a bridge: <https://blogs.oracle.com/javafx/entry/communicating_between_javascript_and_javafx>
	- Change back BrowserWindow and AHrefCriteria(script) to use respectivelly:

```javascript 
html = webEngine.executeScript("document.documentElement.outerHTML").toString

Array.prototype.slice.call(document.getElementsByTagName('a')).map(function(e) { return e.href; });
```

* Don't store more than needed for dns names:
	- Each label may contain up to 63 characters. The full domain name may not exceed the length of 253 characters in its textual representation
	- <https://en.wikipedia.org/wiki/Domain_Name_System>
