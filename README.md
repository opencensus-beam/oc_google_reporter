oc_google_reporter
=====

Reporter for [opencensus]() that implements support for version 1 and 2 of [Google Cloud Trace](https://cloud.google.com/trace/docs/reference/).

### Using

Add dependency to `rebar.config` and the `.app.src` of the application you are adding tracing to:

```
{deps, [opencensus, oc_google_reporter]}.
```

```
{application, ..., [
   {applications, [kernel, stdlib, oc_google_reporter, opencensus, ...]}
   ...
]}
```

Add configuration for `opencensus` is added to `sys.config`, set to use the `oc_google_reporter` or `oc_google_reporter_v2` reporter:

```
{opencensus, [{sampler, {oc_sampler_always, []}},
              {reporter, {oc_google_reporter_v2, #{project_id => <<"GOOGLE PROJECT">>,
                                                   credentials_source => default}}}]}
```

The Google credentials are handled by the [augle](https://github.com/tsloughter/augle) library.
