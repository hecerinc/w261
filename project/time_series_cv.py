def parameter_sets(parameter_grid):
  ''' Create a dictionary combination of the parameter grid '''
  parameter_names = list(parameter_grid.keys())
  v = parameter_grid.values()
  combinations = list(itertools.product(*v))
  return parameter_names, combinations
fixed_params = {
    'family': 'binomial', 
    'featuresCol': STANDARD_FEATURE_COLNAME, 
    'labelCol': STANDARD_LABEL_COLNAME
}
def time_series_CV(dataset, search_space, fixed_params, model, k=3, metric='auc'):
    '''
    k-fold cross validation for time-series
    '''

    # Initiate trackers
    best_score = 0
    best_param_vals = None

    df = dataset
    n = df.count()

    # Assign sequential row IDs based on `FL_DATE` # TODO is this the right col?
    df = df.withColumn('row_id', f.row_number().over(Window.partitionBy().orderBy('FL_DATE')))
    chunk_size = int(n/(k+1))

    parameter_names, parameters = parameter_sets(search_space)

    # assert len(parameters.keys()) >= 1, 'Insufficient parameters entered'

    for p in parameters:
        # Call the model with fixed_params plus the grid params
        estimator = model(*fixed_params, *p)

        # Print parameter set
        param_print = {x[0]:x[1] for x in zip(parameter_names,p)}
        print(f'Parameters: {param_print}')

        # Track score
        scores = []

        # k-folds
        for i in range(k):

            # Do the split
            train_df = df.filter((f.col('row_id') > chunk_size * i)&(f.col('row_id') <= chunk_size * (i+1))).cache()
            test_df  = df.filter((f.col('row_id') > chunk_size * (i+1))&(f.col('row_id') <= chunk_size * (i+2))).cache()

            # Fit the model
            model = estimator.fit(train_df)

            # Predict on held out test set
            test_predictions = model.transform(test_df)

            # Get the score
            evaluator = BinaryClassificationEvaluator(labelCol=STANDARD_LABEL_COLNAME, rawPredictionCol="prediction", metricName='areaUnderROC')
            score = evaluator.evaluate(test_predictions)
            scores.append(score)

            # Set best parameter set to current one for first fold
            if best_param_vals is None:
                best_param_vals = p

        # Take average of all scores
        avg_score = np.average(scores)

        # Update best score and parameter set to reflect optimal test performance
        if avg_score > best_score:
            previous_best = best_score
            best_score = avg_score
            best_parameters = param_print
            best_param_vals = p
            print(f'New best score of {best_score:.2f}')
        else:
            print(f'Result was no better, score was {avg_score:.2f} with best {metric} score {best_score:.2f}')
        print('\n')

    return best_parameters, best_score
