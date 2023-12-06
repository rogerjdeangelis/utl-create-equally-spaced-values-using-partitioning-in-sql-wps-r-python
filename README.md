# utl-create-equally-spaced-values-using-partitioning-in-sql-wps-r-python
Create equally spaced values using partitioning in sql wps r python
    %let pgm=utl-create-equally-spaced-values-using-partitioning-in-sql-wps-r-python;

    Create equally spaced values using partitioning in sql wps r python

    Partitioning can be useful

    Esily solved using a wps datastep dow loop (not shown)

         SOLUTIONS

            1. wps sql
            2. wps r sql
            3. wps python sql
            4. wps r native
                 user:21064546
                 Jianyang Tai
            5. wps sqlpartition macro
            6. partitioning repos


    wps partitioning macro on the end of this post

    github
    https://tinyurl.com/ysbjn5vr
    https://github.com/rogerjdeangelis/utl-create-equally-spaced-values-using-partitioning-in-sql-wps-r-python

    stackoverflow r
    https://tinyurl.com/466xd7kd
    https://stackoverflow.com/questions/77593606/how-to-create-a-equally-spaced-number-within-a-fixed-range-with-a-group-in-r

    Partition macro on end

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
      informat opp_id $9. month $7.;
      input opp_id month count;
    cards4;
    304956938 Oct2023 2
    304956938 Oct2023 2
    305075384 Nov2023 1
    304910225 Dec2023 2
    305101457 Dec2023 2
    305005905 Feb2024 1
    305089124 Mar2024 3
    304955132 Mar2024 3
    304955132 Mar2024 3
    305005359 Jun2024 2
    304904187 Jun2024 2
    304973572 Aug2024 1
    304984865 Sep2024 1
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.HAVE total obs=13                                                                                                  */
    /*                                                                                                                        */
    /*  Obs     OPP_ID       MONTH     COUNT                                                                                  */
    /*                                                                                                                        */
    /*    1    304956938    Oct2023      2                                                                                    */
    /*    2    304956938    Oct2023      2                                                                                    */
    /*    3    305075384    Nov2023      1                                                                                    */
    /*    4    304910225    Dec2023      2                                                                                    */
    /*    5    305101457    Dec2023      2                                                                                    */
    /*    6    305005905    Feb2024      1                                                                                    */
    /*    7    305089124    Mar2024      3                                                                                    */
    /*    8    304955132    Mar2024      3                                                                                    */
    /*    9    304955132    Mar2024      3                                                                                    */
    /*   10    305005359    Jun2024      2                                                                                    */
    /*   11    304904187    Jun2024      2                                                                                    */
    /*   12    304973572    Aug2024      1                                                                                    */
    /*   13    304984865    Sep2024      1                                                                                    */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    options validvarname=any sasautos=("c:/oto");
    proc sql;
      create
        table sd1.want as
      select
         opp_id
        ,month
        ,count
        ,partition/(count+1) as position
      from
        %sqlPartition(sd1.have,by=month)
      group
        by month
    ;quit;
    proc print data=sd1.want;
    run;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs     OPP_ID       MONTH     COUNT      position                                                                     */
    /*                                                                                                                        */
    /*   1    304973572    Aug2024      1               0.5                                                                   */
    /*   2    304910225    Dec2023      2      0.3333333333                                                                   */
    /*   3    305101457    Dec2023      2      0.6666666667                                                                   */
    /*   4    305005905    Feb2024      1               0.5                                                                   */
    /*   5    305005359    Jun2024      2      0.3333333333                                                                   */
    /*   6    304904187    Jun2024      2      0.6666666667                                                                   */
    /*   7    305089124    Mar2024      3              0.25                                                                   */
    /*   8    304955132    Mar2024      3               0.5                                                                   */
    /*   9    304955132    Mar2024      3              0.75                                                                   */
    /*  10    305075384    Nov2023      1               0.5                                                                   */
    /*  11    304956938    Oct2023      2      0.3333333333                                                                   */
    /*  12    304956938    Oct2023      2      0.6666666667                                                                   */
    /*  13    304984865    Sep2024      1               0.5                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    options validvarname=any;
    proc r;
    export data=sd1.have r=have;
    submit;
    library(sqldf);
    want <- sqldf("
      select
         opp_id
        ,month
        ,count
        ,partition
        ,partition/(count+1) as position
      from
         (select opp_id, month, count,  row_number() OVER (PARTITION BY month) as partition from have )
    ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*        opp_id   month count partition  position                                                                        */
    /*                                                                                                                        */
    /*  1  304973572 Aug2024     1         1 0.5000000                                                                        */
    /*  2  304910225 Dec2023     2         1 0.3333333                                                                        */
    /*  3  305101457 Dec2023     2         2 0.6666667                                                                        */
    /*  4  305005905 Feb2024     1         1 0.5000000                                                                        */
    /*  5  305005359 Jun2024     2         1 0.3333333                                                                        */
    /*  6  304904187 Jun2024     2         2 0.6666667                                                                        */
    /*  7  305089124 Mar2024     3         1 0.2500000                                                                        */
    /*  8  304955132 Mar2024     3         2 0.5000000                                                                        */
    /*  9  304955132 Mar2024     3         3 0.7500000                                                                        */
    /*  10 305075384 Nov2023     1         1 0.5000000                                                                        */
    /*  11 304956938 Oct2023     2         1 0.3333333                                                                        */
    /*  12 304956938 Oct2023     2         2 0.6666667                                                                        */
    /*  13 304984865 Sep2024     1         1 0.5000000                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                    _   _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */

    proc datasets nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.have python=have;
    submit;
    print(have);
    import pyreadstat;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
      select
         opp_id
        ,month
        ,count
        ,partition
        ,partition/(count+1) as position
      from
         (select opp_id, month, count,  row_number() OVER (PARTITION BY month) as partition from have )
    ''');
    print(want);
    pyreadstat.write_xport(want,'d:\\xpt\\want.xpt',table_name='want',file_format_version=5
    ,column_labels=['OPP_ID','MONTH','COUNT','PARTITION','POSITION' ]);
    endsubmit;
    ");


    /*--- handles long variable names by using the label to rename the variables  ----*/

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;

    data want_py_long_names;
      %utl_rens(xpt.want) ;
      set want;
    run;quit;
    libname xpt clear;

    proc print data=want_py_long_names;
    format position 5.2;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS Python System                                                                                                 */
    /*                                                                                                                        */
    /*  The PYTHON Procedure                                                                                                  */
    /*                                                                                                                        */
    /*         opp_id    month  count  partition  position                                                                    */
    /*  0   304973572  Aug2024    1.0          1  0.500000                                                                    */
    /*  1   304910225  Dec2023    2.0          1  0.333333                                                                    */
    /*  2   305101457  Dec2023    2.0          2  0.666667                                                                    */
    /*  3   305005905  Feb2024    1.0          1  0.500000                                                                    */
    /*  4   305005359  Jun2024    2.0          1  0.333333                                                                    */
    /*  5   304904187  Jun2024    2.0          2  0.666667                                                                    */
    /*  6   305089124  Mar2024    3.0          1  0.250000                                                                    */
    /*  7   304955132  Mar2024    3.0          2  0.500000                                                                    */
    /*  8   304955132  Mar2024    3.0          3  0.750000                                                                    */
    /*  9   305075384  Nov2023    1.0          1  0.500000                                                                    */
    /*  10  304956938  Oct2023    2.0          1  0.333333                                                                    */
    /*  11  304956938  Oct2023    2.0          2  0.666667                                                                    */
    /*  12  304984865  Sep2024    1.0          1  0.500000                                                                    */
    /*                                                                                                                        */
    /*  WPS                                                                                                                   */
    /*                                                                                                                        */
    /* Obs     OPP_ID       MONTH     COUNT    PARTITION    POSITION                                                          */                                                 */
    /*                                                                                                                        */
    /*   1    304973572    Aug2024      1          1          0.50                                                            */                                                             */
    /*   2    304910225    Dec2023      2          1          0.33                                                            */                                                             */
    /*   3    305101457    Dec2023      2          2          0.67                                                            */                                                             */
    /*   4    305005905    Feb2024      1          1          0.50                                                            */                                                             */
    /*   5    305005359    Jun2024      2          1          0.33                                                            */                                                             */
    /*   6    304904187    Jun2024      2          2          0.67                                                            */                                                             */
    /*   7    305089124    Mar2024      3          1          0.25                                                            */                                                             */
    /*   8    304955132    Mar2024      3          2          0.50                                                            */                                                             */
    /*   9    304955132    Mar2024      3          3          0.75                                                            */                                                             */
    /*  10    305075384    Nov2023      1          1          0.50                                                            */                                                             */
    /*  11    304956938    Oct2023      2          1          0.33                                                            */                                                             */
    /*  12    304956938    Oct2023      2          2          0.67                                                            */                                                             */
    /*  13    304984865    Sep2024      1          1          0.50                                                            */                                                             */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                     _   _ _   _
    | ___|   _ __   __ _ _ __| |_(_) |_(_) ___  _ __   _ __ ___   __ _  ___ _ __ ___
    |___ \  | `_ \ / _` | `__| __| | __| |/ _ \| `_ \ | `_ ` _ \ / _` |/ __| `__/ _ \
     ___) | | |_) | (_| | |  | |_| | |_| | (_) | | | || | | | | | (_| | (__| | | (_) |
    |____/  | .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_||_| |_| |_|\__,_|\___|_|  \___/
            |_|
    */

    %macro sqlPartition(data,by=)/des="emulate sql partition over() funtionality";

      (select
         row_number
        ,row_number - min(row_number) +1 as partition
        ,*
      from
          (select *, monotonic() as row_number from
             /*----                                                          ----*/
             /*----  note max has no effect                                  ----*/
             /*----                                                          ----*/
             (select *, max(%scan(%str(&by),1,%str(,))) as sex from &data group by &by ))
      group
          by &by )

    %mend sqlPartition;

    /*__                     _   _ _   _             _
     / /_   _ __   __ _ _ __| |_(_) |_(_) ___  _ __ (_)_ __   __ _   _ __ ___ _ __   ___  ___
    | `_ \ | `_ \ / _` | `__| __| | __| |/ _ \| `_ \| | `_ \ / _` | | `__/ _ \ `_ \ / _ \/ __|
    | (_)  | |_) | (_| | |  | |_| | |_| | (_) | | | | | | | | (_| | | | |  __/ |_) | (_) \__ \
     \___/ | .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|_|_| |_|\__, | |_|  \___| .__/ \___/|___/
           |_|                                               |___/           |_|
    */

    REPO
    --------------------------------------------------------------------------------------------------------------------------------
    https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
    https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot
    https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning
    https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
    https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
    https://github.com/rogerjdeangelis/utl-transposing-words-into-sentences-using-sql-partitioning-in-r-and-python
    https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
