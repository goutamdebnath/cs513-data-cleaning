# @BEGIN DataCleaningProcess
# @IN input_data_file  @URI file:farmersmarkets-2017-01-10.csv
# @OUT result_map_png  @URI file:Exploratory-Us-Farmers-Market-Map.png
# @OUT result_chart_png  @URI file:Exploratory-US-Farmers-Market-Count-Per-State.png


    # @BEGIN OpenRefineProcess
    # @IN input_data_file  @URI file:file:armersmarkets-2017-01-10.csv
    # @OUT Intermediate_Clean_Data @URI file:farmersmarkets_clean_openrefine.csv
    # @END OpenRefineProcess


    # @BEGIN PythonScript
    # @IN Intermediate_Clean_Data
    # @OUT Final_Clean_Data @URI file:farmersmarkets_clean_openrefine_python.csv
    # @END PythonScript


    # @BEGIN SQL_Constraint_Check
    # @IN Final_Clean_Data
    # @END SQL_Constraint_Check


    # @BEGIN Exploratory_Data_Analysis
    # @IN SQL_Constraint_Check
    # @OUT result_map_png  @URI file:Exploratory-Us-Farmers-Market-Map.png
    # @OUT result_chart_png  @URI file:Exploratory-US-Farmers-Market-Count-Per-State.png
    # @END Exploratory_Data_Analysis


# @END DataCleaningProcess