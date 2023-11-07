<!--
## Contribution Checklist

  - Make sure that the pull request corresponds to a [JIRA issue](https://issues.apache.org/jira/projects/FLINK/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no JIRA issue.
  
  - Name the pull request in the form "[FLINK-XXXX] [component] Title of the pull request", where *FLINK-XXXX* should be replaced by the actual issue number. Skip *component* if you are unsure about which is the best component.

  - Fill out the template below to describe the changes contributed by the pull request.
  
  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message (including the JIRA id)

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.


**(The sections below can be removed for hotfixes of typos)**
-->

## What is the purpose of the change

A short summary of what this pull-request addresses, expanding on the title of the pull-request.

## Brief change log

*(for example:)*
- *The TaskInfo is stored in the blob store on job creation time as a persistent artifact*
- *Deployments RPC transmits only the blob storage reference*
- *TaskManagers retrieve the TaskInfo from the blob cache*

## Verifying this change

One of the following should cover the changes made in this pull-request:

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as describe. Please describe the integration or unit test that covers the given change.

In all cases, ensure that at least a simple integration test with a Snowflake account successfully runs.

## Does this pull request potentially affect one of the following parts:

- Dependencies (does it add or upgrade a dependency): (yes / no)
- The public API, i.e., is any changed class annotated with `@Public(Evolving)`: (yes / no)
- The serializers: (yes / no / don't know)
- The runtime per-record code paths (performance sensitive): (yes / no / don't know)
- Anything that affects delivery guarantees: write, flush, buffering, etc.: (yes / no / don't know)

## Documentation

- Does this pull request introduce a new feature? (yes / no)
- If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
