# Kestra

* Kestra is an orchestration platform that’s highly flexible and well-equipped to manage all types of pipelines
* Kestra provides a user-friendly interface and a YAML-based configuration format, making it easy to define, monitor, and manage workflows.
* How to run Kestra in a docker container:
  ```bash
  docker run --pull=always --rm -it -p 8080:8080 --user=root \\
    -v /var/run/docker.sock:/var/run/docker.sock \\
    -v /tmp:/tmp kestra/kestra:latest server local
  ```

# Flow Properties

### **Properties**

Workflows are referenced as **Flows** and they are declared using YAML. Within each flow, there are 3 required properties you’ll need:

* **id**: which is the name of your flow.
* **Namespace**: This allows you to specify the environments you want your flow to execute in, e.g. production vs development.
* **Tasks**: This is a list of the tasks that will execute when the flow is executed, ***in the order they’re defined in***. Tasks contain an ***id*** as well as a ***type*** with each different type having their own additional properties.

### **Optional Properties**

* **Inputs**: Instead of hardcoding values into your flows, you can set them as constant values separately. Great if you plan to reuse them in multiple tasks.
  Here's how inputs are defined :

  ```yaml
  inputs:
    - id: variable_name
          type: STRING
          defaults: example_string
  ```

  * And then, we can can the input like `{{ inputs.variable_name }}`.
* **Outputs**: Tasks will often generate outputs that you’ll want to pass on to a later task. Outputs let you connect both variables as well as files to later tasks.

  * `{{ outputs.task_id.vars.output_name }}`.
* **Triggers**: Instead of manually executing your flow, you can setup triggers to execute it based on a set of conditions such as time schedule or a webhook.

  * Here’s an example:

  ```yaml
  triggers:
    - id: schedule
      type: io.kestra.core.models.triggers.types.Schedule
      cron: "0 0 * * *"  # Runs daily at midnight

    - id: webhook
      type: io.kestra.core.models.triggers.types.Webhook
      conditions:
        - type: io.kestra.core.models.conditions.types.ExecutionStatusCondition
          status: SUCCESS
  ```

### Flow Example:

```yaml

id: 01_getting_started_data_pipeline
namespace: zoomcamp

inputs:
  - id: columns_to_keep
    type: ARRAY
    itemType: STRING
    defaults:
      - brand
      - price

tasks:
  - id: extract
    type: io.kestra.plugin.core.http.Download
    uri: https://dummyjson.com/products

  - id: transform
    type: io.kestra.plugin.scripts.python.Script
    containerImage: python:3.11-alpine
    inputFiles:
      data.json: "{{outputs.extract.uri}}"
    outputFiles:
      - "*.json"
    env:
      COLUMNS_TO_KEEP: "{{inputs.columns_to_keep}}"
    script: |
      import json
      import os

      columns_to_keep_str = os.getenv("COLUMNS_TO_KEEP")
      columns_to_keep = json.loads(columns_to_keep_str)

      with open("data.json", "r") as file:
          data = json.load(file)

      filtered_data = [
          {column: product.get(column, "N/A") for column in columns_to_keep}
          for product in data["products"]
      ]

      with open("products.json", "w") as file:
          json.dump(filtered_data, file, indent=4)

  - id: query
    type: io.kestra.plugin.jdbc.duckdb.Query
    inputFiles:
      products.json: "{{outputs.transform.outputFiles['products.json']}}"
    sql: |
      INSTALL json;
      LOAD json;
      SELECT brand, round(avg(price), 2) as avg_price
      FROM read_json_auto('{{workingDir}}/products.json')
      GROUP BY brand
      ORDER BY avg_price DESC;
    fetchType: STORE
```
