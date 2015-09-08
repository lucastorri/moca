
## Adding seeds

./target/scala-2.11/moca-0.0.1 -p 1732 -S 127.0.0.1:1731 -s seeds.txt


## Checking results for a specific seed

./target/scala-2.11/moca-0.0.1 -p 1732 -S 127.0.0.1:1731 -r 1

## Seeds file example

```
!& default
!= max-depth 3
!= robots-txt
!= same-domain
!= a-href

http://site0.test:8000|1
```

## TODO

* Re-enable Javascript execution on the JavaFX WebKit browser:
  - it was removed due to issues with the jvm. Getting objects from it seem to make the whole process crash
  - Change back BrowserWindow to use 'html = webEngine.executeScript("document.documentElement.outerHTML").toString'
  - Change back AHrefCriteria to use 'override val script: String = "Array.prototype.slice.call(document.getElementsByTagName('a')).map(function(e) { return e.href; });"'
