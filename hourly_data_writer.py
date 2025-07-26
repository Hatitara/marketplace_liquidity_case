'''
Module to process and aggregate hourly data for demand and supply in a marketplace.
'''
import pandas as pd
import numpy as np

data_folder = "data"

print("Loading marketing data...")

all_marketing_data = pd.read_csv(
    f"{data_folder}/marketing_data.csv", parse_dates=["Week"])
all_marketing_data.columns = (
    all_marketing_data.columns
    .str.strip()
    .str.lower()
    .str.replace(' ', '_')
)
for col in ['expected_spend', 'fact_spend']:
    all_marketing_data[col] = (
        all_marketing_data[col]
        .replace('[\$,]', '', regex=True)
        .astype(float)
    )

print("Loading chat sessions and worktime schedule data...")

all_chat_sessions = pd.read_csv(
    f"{data_folder}/chat_sessions.csv", parse_dates=["event_time", "user_first_chat"])
all_worktime_schedule = pd.read_csv(
    f"{data_folder}/worktime_schedule_data.csv", parse_dates=["event_time"])
all_chat_sessions.drop_duplicates(
    subset=["session_id", "user_id", "expert_id"], inplace=True)
data_threshold = all_marketing_data[all_marketing_data['fact_spend'].isna(
)]['week'].min()

marketing_data_predictive = all_marketing_data[all_marketing_data['week'] >= data_threshold].copy(
)
marketing_data = all_marketing_data[all_marketing_data['week']
                                    < data_threshold].copy()

chat_sessions_predictive = all_chat_sessions[all_chat_sessions['event_time'] >= data_threshold].copy(
)
assert chat_sessions_predictive.empty, "Chat sessions found for impossible date."
chat_sessions = all_chat_sessions[all_chat_sessions['event_time']
                                  < data_threshold].copy()

worktime_predictive = all_worktime_schedule[all_worktime_schedule['event_time'] >= data_threshold].copy(
)
worktime = all_worktime_schedule[all_worktime_schedule['event_time']
                                 < data_threshold].copy()
chat_sessions['hour'] = chat_sessions['event_time'].dt.floor('H')

chat_sessions['time_since_first_chat'] = (
    chat_sessions['event_time'] - chat_sessions['user_first_chat']).dt.total_seconds()
chat_sessions['returning_user_session'] = chat_sessions['time_since_first_chat'] >= 86400
chat_sessions['new_user_session'] = chat_sessions['time_since_first_chat'] < 86400

print("Aggregating chat sessions data...")

demand_df = chat_sessions.groupby('hour').agg(
    chat_sessions=('user_id', 'count'),
    active_users=('user_id', 'nunique'),
    new_user_sessions=('new_user_session', 'sum'),
    returning_user_sessions=('returning_user_session', 'sum'),
    total_session_minutes=('session_duration', 'sum')
).reset_index()

desired_column_order = [
    'hour',
    'active_users',
    'new_user_sessions',
    'returning_user_sessions',
    'chat_sessions',
    'total_session_minutes'
]

demand_df = demand_df[desired_column_order]
worktime['hour'] = worktime['event_time'].dt.floor('H')

print("Aggregating worktime data...")

supply_df = worktime.groupby('hour').agg(
    active_experts=('specialist_astrocrm_user_id', 'nunique'),
    scheduled_minutes=('scheduled_duration', 'sum'),
    online_minutes=('online_worktime_duration', 'sum'),
    busy_minutes=('busy_worktime_duration', 'sum'),
    force_busy_minutes=('force_busy_duration', 'sum')
).reset_index()

print("Merging demand and supply data...")

df_hourly = pd.merge(demand_df, supply_df, on='hour', how='outer')

print("Calculating additional metrics...")

df_hourly['user_to_expert_ratio'] = df_hourly['active_users'] / \
    df_hourly['active_experts'].replace(0, np.nan)
df_hourly['expert_utilization'] = df_hourly['busy_minutes'] / \
    (df_hourly['online_minutes'] +
     df_hourly['busy_minutes']).replace(0, np.nan)

df_hourly['supply_minutes'] = (df_hourly['online_minutes'] +
                               df_hourly['busy_minutes']) - df_hourly['force_busy_minutes']
df_hourly['coverage_rate'] = df_hourly['supply_minutes'] / \
    df_hourly['total_session_minutes'].replace(0, np.nan)

print("Sorting and saving the final hourly data...")

df_hourly = df_hourly.sort_values(by='hour').reset_index(drop=True)
df_hourly.to_csv(f"{data_folder}/hourly_data.csv", index=False)

print(f"Data processing complete. Hourly data saved to '{data_folder}/hourly_data.csv'.")
