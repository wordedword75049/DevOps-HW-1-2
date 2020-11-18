# Entities

1. Source
1. Batch
1. Candidate

# REST Resources

See [RESTful API standards & conventions](https://bostongene.atlassian.net/wiki/spaces/PLT/pages/80944838/RESTful+API+standards+conventions).

## ``Sources`` belongs to ``Source`` entity

Endpoint ``GET /sources``:

```yaml
sources:
- id: nct
  label: NCT
  _links:
    - id: self
      href: /sources/nct
- id: conference
  label: Conference
  _links:
    - id: self
      href: /sources/conference
```

Endpoint ``GET /sources/:id``:

```yaml
id: nct
label: NCT
useBatchMode: true
_links:
  - id: self
    href: /sources/nct
  - id: batches
    href: /batches?source=nct{&page=0&size=20&sort=-createDate}
  - id: latest-batch
    href: /batches/30
  - id: latest-candidates
    href: /candidates?source=nct&batchId=30{&page=0&size=10&sort=+candidateId}
```

or

```yaml
id: conference
label: Conference
useBatchMode: false
_links:
  - id: self
    href: /sources/conference
```

## ``Batches`` belongs to ``Batch`` entity

Batch entity spec:

```yaml
id: 30
sourceRefNo: nct
label: NCT <from date> to <to date>
createDate: 2020-01-11T10:55:33
fromDate: 2020-01-10
toDate: 2020-01-31
_links:
  - id: self
    href: /batches/30
  - id: candidates
    href: /candidates?source=nct&batchId=30{&page=0&size=10&sort=+candidateId}
```
