"""

Google Facets utility functions.

"""

from IPython.core.display import display, HTML
from hopsfacets import generic_feature_statistics_generator as generic_feature_statistics_generator
import base64

def dive(jsonstr):
    """ Display json string in HTML

    Args:
        :jsonstr: json string
    """
    HTML_TEMPLATE = """<link rel="import" href="/nbextensions/facets-dist/facets-jupyter.html">
            <facets-dive id="elem" height="600"></facets-dive>
            <script>
              var data = {jsonstr};
              document.querySelector("#elem").data = data;
            </script>"""
    html = HTML_TEMPLATE.format(jsonstr=jsonstr)
    display(HTML(html))    
    
def overview(train_data, test_data):
    """
    Calculate the feature statistics proto from the datasets and stringify it and display in facets overview

    Args:
        :train_data: the train data
        :test_data: the test data
    """
    # Calculate the feature statistics proto from the datasets and stringify it for use in facets overview
    gfsg = generic_feature_statistics_generator.GenericFeatureStatisticsGenerator()
    proto = gfsg.ProtoFromDataFrames([{'name': 'train', 'table': train_data},
                                      {'name': 'test', 'table': test_data}])
    protostr = base64.b64encode(proto.SerializeToString()).decode("utf-8")
    # Display the facets overview visualization for this data
    HTML_TEMPLATE = """<link rel="import" href="/nbextensions/facets-dist/facets-jupyter.html" >
            <facets-overview id="elem"></facets-overview>
            <script>
              document.querySelector("#elem").protoInput = "{protostr}";
            </script>"""
    html = HTML_TEMPLATE.format(protostr=protostr)
    display(HTML(html))
