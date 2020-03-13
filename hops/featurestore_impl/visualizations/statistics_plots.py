"""
Utility functions for plotting the feature statistics stored in Hopsworks for Feature Groups and Training Datasets
"""

import numpy as np
import math
import pandas as pd

# for backwards compatibility
try:
    import matplotlib.pyplot as plt
    from matplotlib import rc
    # Set the global font to be DejaVu Sans, size 10 (or any other sans-serif font of your choice!)
    rc('font',**{'family':'sans-serif','sans-serif':['DejaVu Sans'],'size':10})
    # Set the font used for MathJax - more on this later
    rc('mathtext',**{'default':'regular'})
    # The following %config line changes the inline figures to have a higher DPI.
    # You can comment out (#) this line if you don't have a high-DPI (~220) display.
    import seaborn as sns
except:
    pass


def _plot_feature_distribution(ax, feature_distribution, color='lightblue', log=False, align="center"):
    """
    Creates a single bar plot for a feature distribution

    Args:
        :ax: the axes object to add the plot to
        :feature_distribution: the feature distribution to plot
        :color: the color of the bar plot
        :log: whether to use logarithmic scaling on the y-axes
        :align: how to align the bars, defaults to center.

    Returns:
        None

    """
    frequency_distribution = feature_distribution.frequency_distribution
    frequencies = list(map(lambda feature_frequency: int(feature_frequency.frequency), frequency_distribution))
    bins = list(map(lambda feature_frequency: float(feature_frequency.bin), frequency_distribution))
    width = np.min(np.diff(bins))
    ax.bar(bins, frequencies, color=color, width=width, align=align, log=log)


def _plot_feature_correlations(ax, correlation_matrix, cmap="coolwarm", annot=True, fmt=".2f", linewidths=.05):
    """
    Creates a heatmap plot of the feature correlations

    Args:
        :ax: the axes object to add the plot to
        :correlation_matrix: the feature correlations
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot

    Returns:
        The heatmap
    """
    hm = sns.heatmap(correlation_matrix, ax=ax, cmap=cmap, annot=annot, fmt=fmt,
                     linewidths=linewidths)
    return hm


def _plot_feature_clusters(ax, cluster_analysis):
    """
    Creates a scatter plot of the feature clusters

    Args:
        :ax: the axes object to add the plot to
        :cluster_analysis: the cluster analysis data to plot

    Returns:
        None

    """
    data, colors, groups = _get_cluster_data(cluster_analysis)
    for dataset, color, group in zip(data, colors, groups):
        x, y = dataset
        ax.scatter(x, y, alpha=0.8, c=color, edgecolors='none', label=group)


def _stylize_axes(ax, title, xlabel, ylabel):
    """
    Customize axes spines, title, labels, ticks, and ticklabels.

    Args:
        :ax: the axes to customize
        :title: the title to set
        :xlabel: the label on the x axis
        :ylabel: the label on the y axis

    Returns:
        None
    """
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.xaxis.set_tick_params(top=False, direction='out', width=1)
    ax.yaxis.set_tick_params(right=False, direction='out', width=1)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)


def _create_correlation_matrix(feature_correlations):
    """
    Creates a pandas dataframe with the featurecorrelations from the JSON data returned from Hopsworks REST API
    The pandas dataframe can then be used directly to produce the heatmap plot

    Args:
        :feature_correlations: the feature correlations in the format stored in the DB and returned by Hopsworks

    Returns:
        A pandas dataframe with the feature correlations

    """
    data = {}
    index = [fc.feature_name for fc in feature_correlations]

    def _get_fc(feature_correlations, name):
        return list(filter(lambda x: x.feature_name == name, feature_correlations))[0]

    def _get_corr(correlation_values, name):
        return list(filter(lambda x: x.feature_name == name, correlation_values))[0].correlation

    for index_feature in index:
        for index_feature2 in index:
            fc = _get_fc(feature_correlations, index_feature2)
            if index_feature2 in data:
                data[index_feature2].append(_get_corr(fc.correlation_values, index_feature))
            else:
                data[index_feature2] = [_get_corr(fc.correlation_values, index_feature)]
    data["index"] = index
    correlation_matrix = pd.DataFrame(data)
    correlation_matrix = pd.pivot_table(correlation_matrix, index="index")
    correlation_matrix.index.name = None
    return correlation_matrix


def _get_cluster_data(cluster_analysis):
    """
    Extracts the cluster analysis data into a format suitable for scatter plot with matplotlib
    Args:
        :cluster_analysis:

    Returns:
        data, colors, groups
    """

    def _get_cluster(datapoint, clusters):
        return list(filter(lambda x: x.datapoint_name == datapoint.name, clusters))[0].cluster

    def _unique_clusters(clusters):
        return list(set(list(map(lambda x: x.cluster, clusters))))

    all_colors = ["red", "green", "blue", "orange", "black", "purple", "green"]

    u_clusters = _unique_clusters(cluster_analysis.clusters)
    clusters = cluster_analysis.clusters
    data_points = cluster_analysis.datapoints

    colors = []
    data = []
    groups = []
    for idx, cluster in enumerate(u_clusters):
        color = all_colors[idx]
        data_points_x = []
        data_points_y = []
        filtered_dps = list(filter(lambda dp: _get_cluster(dp, clusters) == cluster, data_points))
        for dp in filtered_dps:
            data_points_x.append(dp.first_dimension)
            data_points_y.append(dp.second_dimension)
        colors.append(color)
        data.append((data_points_x, data_points_y))
        groups.append("cluster " + str(cluster))
    colors = tuple(colors)
    groups = tuple(groups)
    return data, colors, groups


def _visualize_feature_distributions(feature_distributions, figsize=None, color='lightblue', log=False,
                                     align="center"):
    """
    Visualizes the feature distributions of a training dataset or feature group in the featurestore

    Args:
        :feature_distributions: the feature distributions to visualize as histograms
        :figsize: the size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.

    Returns:
        The figure
    """

    # Create the Raw sub-figures
    ncols = 2
    num_distributions = len(feature_distributions)
    nrows = math.ceil(num_distributions / ncols)

    if figsize is None:
        figsize = (16, nrows * 4)

    fig, ax = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)
    titles = []

    for plot_number in range(num_distributions):
        _plot_feature_distribution(ax.flat[plot_number], feature_distributions[plot_number], color=color, log=log,
                                   align=align)
        titles.append(feature_distributions[plot_number].feature_name)

    # Stylize the sub-figures
    xlabel = 'Bin'
    ylabel = 'Frequency'
    for i, axes in enumerate(ax.flat):
        if (i < len(titles)):
            _stylize_axes(axes, titles[i], xlabel, ylabel)

    return fig


def _visualize_feature_correlations(feature_correlations, figsize=(16,12), cmap="coolwarm", annot=True,
                                    fmt=".2f", linewidths=.05):
    """

    Visualizes the feature correlations of a training dataset or feature group in the featurestore

    Args:
        :feature_correlations: the feature correlations
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot

    Returns:
        the figure
    """
    fig, (ax) = plt.subplots(1, 1, figsize=figsize)
    corr_matrix = _create_correlation_matrix(feature_correlations)
    _plot_feature_correlations(ax, corr_matrix, cmap=cmap, annot=annot, fmt=fmt, linewidths=linewidths)
    return fig


def _visualize_feature_clusters(cluster_analysis, figsize=(16,12)):
    """

    Visualizes the feature clusters of a training dataset or feature group in the featurestore

    Args:
        :cluster_analysis: the feature correlations
        :figsize: the size of the figure

    Returns:
        the figure
    """
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(1, 1, 1, facecolor="1.0")
    _plot_feature_clusters(ax, cluster_analysis)
    plt.title('Cluster analysis')
    plt.legend(loc=2)
    return fig


def _visualize_descriptive_stats(descriptive_stats):
    """

    Visualizes the descriptive statistics of a training dataset or feature group in the featurestore

    Args:
        :descriptive_stats: the descriptive statistics

    Returns:
        a pandas dataframe with the statistics
    """
    data = {}
    metrics = []
    features = []
    for metric_values in descriptive_stats:
        features.append(metric_values.feature_name)
        for metric_value in metric_values.metric_values:
            metrics.append(metric_value.metric_name)

    metrics = list(set(metrics))
    features = list(set(features))
    data["metric"] = metrics

    def _get_metric_value_for_feature(metric, feature, descriptive_stats):
        for metric_values in descriptive_stats:
            if metric_values.feature_name == feature:
                for metric_value in metric_values.metric_values:
                    if metric_value.metric_name == metric:
                        return metric_value.value

    for metric in metrics:
        for feature in features:
            if feature in data:
                data[feature].append(_get_metric_value_for_feature(metric, feature, descriptive_stats))
            else:
                data[feature] = [_get_metric_value_for_feature(metric, feature, descriptive_stats)]

    desc_stats_df = pd.DataFrame(data)
    desc_stats_df.set_index("metric")
    return desc_stats_df