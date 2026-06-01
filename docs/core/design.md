# Design

This is the design document for overall architecture of the beacon package.
It will cover the high level design and the rationale behind it.

## Overall Architecture

```text
User --> Client Workflow            --> Runner --> Server / Local --> Map Queue --> Execute --> Return Result
         (workflow = Workflow(...))
```
