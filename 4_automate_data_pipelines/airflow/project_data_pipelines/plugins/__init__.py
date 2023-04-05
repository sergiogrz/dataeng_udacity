from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from plugins import operators, helpers

# import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
    ]
    helpers = [helpers.SqlQueries]
