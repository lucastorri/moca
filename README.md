
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

