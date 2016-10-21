import numpy as np


def temptable_template(df, sql, DB):

    """
    This function takes a dataframe, a stub bit of a bulk sql insert e.g.

    INSERT INTO mytable VALUES

    and turns the datafame into the requisite string that follows and concatenates it all together.

    :param df: The dataframe we are going to bulk insert
    :param sql: The stub sql for the bulk insert.
    :param DB: An instance of the PostgresManager class
    :return:
    """

    df_array = df.values
    segments_shape = np.shape(df_array)

    '''
    This complicated statement takes a 2d numpy array
     ((1,2,3,4),
      (5,6,7,8)
      (9,1,2,3))
    and turns it into a string that looks like

    '((1,2,3,4),(5,6,7,8),(9,1,2,3))'

    for use in bulk sql inserts
    '''

    records_list_template = (','.join(['('+(','.join(['%s'] * segments_shape[1]))+')']*segments_shape[0]))
    sql += records_list_template+';'

    sql = DB.query_mogrify(sql, sql_var=df_array.flatten())

    return sql
