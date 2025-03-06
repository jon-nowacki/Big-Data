def highlight_cell(val, column_name):
    # dictionary flag
    thresholds = {
        "PCT_FRCST_NULL": [(0.5, 'background-color: red'), (0.4, 'background-color: yellow')],
        "PCT_BOH_NULL": [(0.01, 'background-color: red'), (0.001, 'background-color: yellow')],
        "PCT_OOS_NULL": [(0.06, 'background-color: red'), (0.05, 'background-color: yellow')],
        "PCT_AD_BLCK_NULL": [(0.01, 'background-color: red'), (0.001, 'background-color: yellow')],
        "PCT_LOST_SALES_NULL": [(0.1, 'background-color: red'), (0.08, 'background-color: yellow')],
        "ESR_DIFF": [(0, 'background-color: red', True)]
    }
    
    if column_name in thresholds:
        for threshold, color, exact in thresholds[column_name]:
            if (exact and val != threshold) or (not exact and val >= threshold):
                return color
    return ''

pandas_df = df_pct.toPandas()

styled_df = pandas_df.style.apply(
    lambda x: [highlight_cell(v, x.name) for v in x], 
    axis=0
)
display(styled_df)
