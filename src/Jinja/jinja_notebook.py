# Databricks notebook source
pip install jinja2

# COMMAND ----------

from jinja2 import Template

parameters = [
    {
       "table" : "spotify_catalog`.`silver-layer`.`factstream",
       "alias" : "factstream",
       "cols" : "factstream.stream_id, factstream.listen_duration"
    },
    {
       "table" : "spotify_catalog`.`silver-layer`.`dimuser",
       "alias" : "dimuser",
       "cols" : "dimuser.user_id, dimuser.user_name",
       "condition" : "factstream.user_id = dimuser.user_id"
    },
    {
       "table" : "spotify_catalog`.`silver-layer`.`dimtrack",
       "alias" : "dimtrack",
       "cols" : "dimtrack.track_id, dimtrack.track_name",
       "condition" : "factstream.track_id = dimtrack.track_id"
    },
]

# COMMAND ----------

query_text = """
SELECT
    {% for param in parameters %}
        {{ param.cols }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM
    `{{ parameters[0].table }}`
    AS {{ parameters[0].alias }}
    {% for param in parameters[1:] %}
    LEFT JOIN `{{ param.table }}`
    AS {{ param.alias }}
    ON {{ param.condition }}
    {% endfor %}
"""


# COMMAND ----------

spark.sql("USE CATALOG spotify_catalog")

# COMMAND ----------

jinja_sql_str = Template(query_text)
query= jinja_sql_str.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))