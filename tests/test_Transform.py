import pandas as pd
import numpy as np
import logging
from unittest import TestCase, mock, main
from krossy import Transform, EmptyDfError


#python -m unittest tests/test_Transform.py -v


class TestTransform(TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger()
        self.transform = Transform()
        self.df = pd.DataFrame({})


    def test_drop_none_missed_items(self):
        """
        Check if method logs info.
        """
        
        with mock.patch.object(self.logger, 'info') as mock_logger:
            df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
            self.transform.drop_none(df=df)

            mock_logger.assert_called_with(
                f'Number of missing items: {0}'
            )


    def test_drop_none_empty_df(self):
        """
        Check if method raises exception when dataframe is empty.
        """

        with self.assertRaises(EmptyDfError) as e:
            self.transform.drop_none(df=self.df)
        
    
    def test_drop_none_empty_df_log(self):
        """
        Check if method logs error when dataframe is empty.
        """

        with mock.patch.object(self.logger, 'error') as mock_logger:
            try:
                self.transform.drop_none(df=self.df)
                mock_logger.assert_called_with(
                'Empty extract df. Check internet connection or urls'
            )
            except EmptyDfError:
                pass
    

    def test_add_another_current(self):
        """
        Check adding another current to dataframe.
        """

        df = pd.DataFrame({'price_ua': [350]})
        self.transform.add_another_current(df=df)

        self.assertTrue(df.equals(
            pd.DataFrame({'price_ua': [350], 'price_us': [10.0],})
        ))


    def test_add_datetime(self):
        """
        Check adding date column to dataframe.
        """

        df = pd.DataFrame({'col1': [1]})
        self.transform.add_data_time(df=df)

        self.assertTrue(np.issubdtype(df['date'].dtype, np.datetime64))


if __name__ == '__main__':
    main()