import matplotlib.pyplot as plt
import pandas as pd
import os
import tqdm

# These two locations contain parquet files with the data we need
# They can be generated using the data_retrieval.py script and the get_total_posts_per_week.py script on a Spark cluster
TAGS_PATH = "tags_output"
POSTS_PATH = "total_posts_per_week"

def create_dataframe_from_path(path):
    # List all Parquet files in the directory
    parquet_files = [file for file in os.listdir(path) if file.endswith('.parquet')]

    # Initialize an empty list to store individual DataFrames
    dfs = []

    # Loop through Parquet files and read each into a DataFrame
    for file in parquet_files:
        file_path = os.path.join(path, file)
        df = pd.read_parquet(file_path)
        dfs.append(df)

    # Concatenate the individual DataFrames into one
    return pd.concat(dfs, ignore_index=True)

# Get data
tags_output = create_dataframe_from_path(TAGS_PATH)
posts_output = create_dataframe_from_path(POSTS_PATH)
posts_output = pd.DataFrame(sorted(posts_output.values, key=lambda x: x[0]), columns=posts_output.columns)

# Extract tags and week_counts
tags = tags_output['tags']

# Use this variable to change what you want the program to do. Only use one of the following:
#   calculate: calculates the slopes and trends
#   show: shows the most popular graphs and the ones specified in interesting_tags
#   save: saves the most popular graphs and the ones specified in interesting_tags

# variant = 'calculate'
variant = 'show'
# variant = 'save'

# We define interesting tags to show or save
interesting_tags = [
    'python',
    'pyspark',
    'hadoop',
    'apache-spark',
    'mapreduce',
    'bigdata',
    'time-series',
    'pandas',
    'numpy',
    'matplotlib',
    'parquet',
    'xml',
    'openai-api',
    'pytorch',
]

slopes_df = pd.DataFrame(columns=['tag', 'slope', 'trend'])

for tag in tqdm.tqdm(tags.unique()):
    if tag != 'python':
        continue

    tag_data = tags_output[tags == tag]

    if tag_data['week_counts'].iloc[0].size < 52:
        # There are less then 52 weeks with data, so we don't use this tag
        continue

    # Get the counts per week for this tag
    week_counts = pd.json_normalize(tag_data['week_counts'].explode())
    week_counts['week'] = pd.to_datetime(week_counts['week'])
    week_counts = pd.DataFrame(sorted(week_counts.values, key=lambda t: t[0]), columns=week_counts.columns)

    # Find overlapping data between the tag and the total posts
    common_weeks = week_counts['week'].isin(posts_output['week'])
    posts_common = posts_output[posts_output['week'].isin(week_counts[common_weeks]['week'])]

    # Normalize the post counts using the total post counts for that week
    y_normalized = pd.DataFrame([y_val / y_2_val for y_val, y_2_val in zip(week_counts['count'].to_list(), posts_common['count'].to_list())], columns=['Normalized post count'])

    # Create a DataFrame with all the data
    df_tag_trends = pd.concat([posts_common['week'].reset_index(), y_normalized], axis=1)

    if variant in ["show","save"] and df_tag_trends['Normalized post count'].mean() < 0.01 and tag not in interesting_tags:
        # For showing and saving images, we only want very popular tags or tags we find interesting
        continue

    # Calculate the rolling average of the normalized post count
    df_tag_trends['Rolling average of normalized post count'] = y_normalized.rolling(26, center=True).mean().dropna()

    # Calculate the rolling average of the difference of the rolling average of the normalized post count
    df_tag_trends['Differential of normalized post count'] = y_normalized.rolling(26, center=True).mean().diff().rolling(26, center=True).mean().dropna()

    # This threshold is used to determine whether the trend is positive or negative
    threshold = df_tag_trends['Differential of normalized post count'].std() / 4

    if variant in ["show", "save"]:
        # Plot the two lines
        df_tag_trends.plot(x='week', y=['Normalized post count', 'Rolling average of normalized post count'], figsize=(20,10), linewidth=5, fontsize=20)
        
        # Remove label on x-axis
        plt.xlabel('')

        # We create a column for the trend areas in the graph
        df_tag_trends['trend'] = df_tag_trends['Differential of normalized post count'].apply(lambda x: 'positive' if x > threshold else 'negative' if x < -threshold else 'changing')    

        # Create rectangles for these areas
        start = None
        previous_trend = 'none'
        for i, row in df_tag_trends.iterrows():
            if start is None:
                start = row['week']

            if row['trend'] != previous_trend:
                if previous_trend == 'positive':
                    plt.fill_between([start, df_tag_trends.iloc[i-1]['week']], df_tag_trends['Normalized post count'].min(), df_tag_trends['Normalized post count'].max(), color='green', alpha=0.3)
                    start = df_tag_trends.iloc[i-1]['week']
                elif previous_trend == 'negative':
                    plt.fill_between([start, df_tag_trends.iloc[i-1]['week']], df_tag_trends['Normalized post count'].min(), df_tag_trends['Normalized post count'].max(), color='red', alpha=0.3)
                    start = df_tag_trends.iloc[i-1]['week']
                elif previous_trend == 'changing':
                    start = df_tag_trends.iloc[i-1]['week']

            previous_trend = row['trend']

        # Also fill the last part
        if previous_trend == 'positive' and start is not None:
            plt.fill_between([start, df_tag_trends['week'].max()], df_tag_trends['Normalized post count'].min(), df_tag_trends['Normalized post count'].max(), color='green', alpha=0.3)
        elif previous_trend == 'negative':
            plt.fill_between([start, df_tag_trends['week'].max()], df_tag_trends['Normalized post count'].min(), df_tag_trends['Normalized post count'].max(), color='red', alpha=0.3)

    # Determine whether the trend is increasing or decreasing at the end (last year)
    last_year = df_tag_trends.tail(52)
    last_year_slope = last_year['Differential of normalized post count'].mean()

    # We still use the same threshold
    trend = 'neutral'
    if last_year_slope >= threshold:
        trend = 'positive'
    elif last_year_slope <= -threshold:
        trend = 'negative'

    # Set the title of the graph
    plt.title(f"{tag} ({trend})", fontsize=20)

    # Add info to the end of slopes_df
    slopes_df = slopes_df._append({'tag': tag, 'slope': last_year_slope, 'trend': trend}, ignore_index=True)

    if variant == "show":
        plt.show()
    elif variant == "save":
        plt.savefig(f"trends/{tag}.png")

if variant == "calculate":
    # Sort slopes_df by slope
    slopes_df.sort_values(by=['slope'], inplace=True)

    # Export slope DataFrame to CSV
    slopes_df.to_csv('slopes.csv', index=False)