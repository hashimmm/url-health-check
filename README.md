# SITEMONITOR

## Notes

There's a function to get metrics, which works, but it isn't really exposed via any API, http or cmdline or otherwise. The producer and consumer and tests work. And the tests take a while since they test the whole round trip.

Must populate `settings.py` for things to work, btw. Also, CA cert files are created during running/testing, from the strings inside `settings.py`. 


### Running for development:

```
docker-compose -f .\docker-compose-dev.yml build
docker-compose -f .\docker-compose-dev.yml run sitemonitor
```

The above will drop you into bash.

#### Testing

From inside the bash prompt, run `python /code/test.py`

Tests require internet to work, and assume "https://www.example.com/" to be working and reachable.

A very neat idea would be to run a "simplehttpserver" in python so the dependency on "example.com" would go away, but I was still connecting to the aiven postgres and kafka so didn't bother.


### Running consumer and producer:

`docker-compose -f .\docker-compose-run.yml up -d --build`
