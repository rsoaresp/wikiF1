#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 19:46:50 2018

@author: rsoaresp@gmail.com
"""

import numpy as np
import pandas as pd


class Formula1:
    """A simple class that builds a pandas df with Formula 1 pilot's info
    from the wikipedia.
    
    It uses the read_html function of pandas to get a table from Wikipedia with
    information about all the formula 1 drives to date and many interesting
    statistics related to it. After loading the table, we use simple regex rules
    to clean up the data and put it in a readable form.
    """

    def __init__(self):

        self.link = "https://en.wikipedia.org/wiki/List_of_Formula_One_drivers"
        self.table = None

    def popTable(self):
        """Populates the data frame with the information.
        
        After creating the class, call this method to access the wikipedia's page
        and does all the parsing of the information.
        """

        print("Reading the table of F1 pilots from wikipedia...")
        self.table = pd.read_html(self.link, header=0)[2][:-1]

        # The first thing we will do is to remove the citations that may be
        # in the data. In order to do this we will use regex, searching for
        # patterns in the form '[]' with a number between the square brackets.
        for i in self.table.columns:
            self.table[i] = self.table[i].str.replace("\\[.*\\]", "")

        # Now we will split the data in the 'Seasons' column using '–' or a
        # collon and a space, ', '. After this we will pick up the first and the
        # last elements found, to be the first and last seaasons of the pilot.
        seasons = self.table["Seasons"].str.split("–|, ")

        self.table["First season"] = seasons.str.get(0)
        self.table["Last season"] = seasons.str.get(-1)

        # In the wikipedia page there are some special symbols that identifies
        # pilots that are champions, for example. As we do not need this info,
        # we just remove these symbols from the names.
        self.table["Name"] = self.table["Name"].str.replace("~|\\*|\\^", "")

        # As the loading of the table mixes up some information on the html,
        # we have to split the columns with the name and put it on the right
        # order, first the name and then the surname.
        name = self.table["Name"].str.rsplit(", ", n=1).str.get(1).fillna("")
        name = name.str.rsplit(" ", n=1).str.get(0)

        surname = self.table["Name"].str.rsplit(", ", n=1).str.get(0)

        self.table["Name"] = name.map(lambda x: x[0 : int(len(x) / 2)]) + " " + surname
        self.table["Name"] = self.table["Name"].str.strip()

        # For the champions, we include a column with the seasons of the championships won.
        temp = self.table.loc[:, "Championships"].copy()

        self.table["Championships"] = temp.str[0]
        self.table["Championship years"] = temp.str[1:]

        # Finally we convert some columns from strings to numbers, such that
        # we can use then latter to extract useful information with pandas.
        colsConvert = [
            "Entries",
            "Starts",
            "Poles",
            "Wins",
            "Podiums",
            "Fastest laps",
            "Points[note]",
            "First season",
            "Last season",
            "Championships",
        ]

        self.table[colsConvert] = self.table[colsConvert].apply(
            pd.to_numeric, errors="coerce", axis=1
        )
