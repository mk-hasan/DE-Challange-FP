"""
Author: kamrul Hasan
Date: 05.12.2021
Email: hasan.alive@gmail.com
"""


"""
This module will transform the data little bit using some aggregation by pulling the data from postgres and then generate a static html report. 
"""

import pandas.io.sql as psql
from pandas.io.formats.style import jinja2
from pandasql import sqldf
import seaborn as sns
import utlilities.utility_factory
from src.db_engine import DBFactory


class GenerateReport():

    def __init__(self, db_isntance) -> None:
        self.db_isntance = db_isntance
        self.pysqldf = lambda q: sqldf(q, globals())
        self.final_df = None

    @utlilities.utility_factory.DecoratorFactory.program_status
    def transform_data(self) -> None:
        """
        This function will transform the data by doing inner join and some aggregation.
        :return:
        """
        db_instance.connect()

        cursor = db_instance.conn.cursor()

        # Do some joining and aggregation to find out the top three parameter related to product and their production.
        query  = """
                SELECT tmp.param_id,tmp.product,tmp.production
                FROM (SELECT * FROM metrics_data md
                inner join worknode_data wd
                on md.time = wd.time) as tmp
                group by tmp.product,tmp.param_id,tmp.production
                """
        new_df = psql.read_sql(query, db_instance.conn)

        new_df["rank"] = new_df.groupby("product")["production"].rank("dense", ascending=False)
        new_df_rank = new_df[new_df["rank"] <= 3.0]
        self.final_df = new_df_rank.sort_values(by=['product', 'production'])

    @utlilities.utility_factory.DecoratorFactory.program_status
    def generate_static_report(self) -> None:
        """
        This function will generate simple static report with final processed data.
        :return: None
        """
        self.transform_data()
        styler = self.final_df.style.applymap(utlilities.utility_factory.color_negative_red)
        # Template handling
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath='./../'))
        template = env.get_template('/report/template.html')
        html = template.render(my_table=styler.render())

        # Plot
        ax = sns.barplot(x='param_id', y='production', data=self.final_df, hue='product')
        fig = ax.get_figure()
        fig.savefig('./../report/plot.svg')

        # Write the HTML file
        with open('./../report/report.html', 'w') as f:
            f.write(html)


if __name__ == '__main__':
    # read and parse the config json file
    config_data = utlilities.utility_factory.load_config()
    db_instance = DBFactory(config_data['db_hostname'], config_data['db_port'], config_data['db_name'],
                            config_data['db_username'], config_data['db_password'])
    gr = GenerateReport(db_instance)
    gr.generate_static_report()
