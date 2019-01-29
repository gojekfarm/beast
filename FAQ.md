# F.A.Q

### Deployment Setup

Beast instances are deployed deployed on kubernetes (as of now). Each beast pod two containers - `beast` & `telegraf`.
For deploying beast on kubernetes, we need:
* Beast
  * Deployment
  * ConfigMap
  * Secret - containing credentials to BigQuery (is shared by all beast deployments)
* Telegraf
  * Configmap - shared by all telegraf containers in the entire namespace

### Telegraf as a sidecar

For most use cases one can use the grobal telegraf instance, and push metrics directly from the app to telegraf via statsd. Though in case of Java applications, if we require JMX metrics, we need a mechanism to pull metrics from the app.

Configuring the global telegraf to pull metrics from individual application pods can be cumbersome (not sure if it's even feasible). To resolve this issue, we introduced telegraf as a sidecar.

Each app pod has an app container, and a telegraf container. The following are the flows for different type of metrics:

* Process Metrics
Jolokia jar is included in the app docker image in the CI pipeline itself. The JVM in app container exposes JMX metrics. Inside the app container itself, jolokia exposes these metrics via a http endpoint. Telegraf container pulls these metrics from this exposed endpoint (via the jolokia input plugin for telegraf). Telegraf then pushes this data to InfluxDB.

* Business Metrics
The app pushes the business metrics directly to the global telegraf instane via statsd client. Telegraf as usual, pushes the data to InfluxDB.

#### Steps to add telegraf as a sidecar in another java app:

* In your app's CI pipeline, include the jolokia jar. For reference, take a look at Beast's [gitlab-ci.yaml](.gitlab-ci.yml).
* In your manifest/helm-chart, include a `configmap` for telegraf. This can be common for all telegraf containers of your app deployment.
* In the app deployment itself, mount the telegraf configmap inside the telegraf container using `volumeMount`.
* For kebernetes deployment reference, take a look at Beast's [helm-chart](https://gitlab.com/data_engineering/infrastructure/scrolls/tree/master/helm-charts/beast)
