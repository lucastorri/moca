
## Adding seeds

```bash
./target/scala-2.11/moca-0.0.1 -p 1732 -S 127.0.0.1:1731 -s seeds.txt
```


## Checking results for a specific seed

```bash
./target/scala-2.11/moca-0.0.1 -p 1732 -S 127.0.0.1:1731 -r 1
```

## Seeds file example

```
!& default
!= max-depth 3
!= robots-txt
!= same-domain
!= a-href

http://site0.test:8000|1
```

## Set Up PostgreSQL for `akka-persistence-sql-async`

Run the following on your shell:

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


## TODO

* Re-enable Javascript execution on the JavaFX WebKit browser:
  - it was removed due to issues with the jvm. Getting objects from it seem to make the whole process crash
  - try by using a bridge: <https://blogs.oracle.com/javafx/entry/communicating_between_javascript_and_javafx>
  - Change back BrowserWindow and AHrefCriteria(script) to use respectivelly:

```javascript 
html = webEngine.executeScript("document.documentElement.outerHTML").toString

Array.prototype.slice.call(document.getElementsByTagName('a')).map(function(e) { return e.href; });
```

