def save_data(df, save_path):
    (
        df.coalesce(1)
        .write
        .option('header', 'true')
        .csv(save_path)
    )