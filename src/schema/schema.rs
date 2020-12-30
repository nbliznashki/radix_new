///
///How does a join of two tables work?
///E.g.
///SELECT T1.CUS_NO, T2.CUS_ID
///FROM T1 LEFT JOIN T2 ON T1.CUS_NO=T2.CUS_NO AND (T1.CUS_ID>=T2.CUS_ID OR T2.CUS_ID IS NULL)
///WHERE T1.CUS_ID>0

/// 0) T1.CUS_NO, T2.CUS_NO, T1.CUS_ID, T2.CUS_ID are stored like OwnedColumns without an index
/// 1) Take references R_T1_CUS_NO, R_T2_CUS_NO, R_T1_CUS_ID, R_T2_CUS_ID and store them in tables T1 and T2
/// 2) Partition T1 and T2, refer to these columns as P_T1_CUS_NO, P_T2_CUS_NO, P_T1_CUS_ID, P_T2_CUS_ID

///Ops to be implemented only for Columns
///Index part of the table and not of the column
///Bitmap part of the column
///Null bitmap => Default value in column

/// 3) Initialize a partitioned index column P_I_T1 by applying {if (P_T1_CUS_ID>0) then store value}
/// 4) Calculate P_H_T1 using (P_T1_CUS_NO, P_I_T1)
/// 5) Re-Partition (P_T2_CUS_NO, P_I_T1) using P_H_T1 mod 1024
/// 6) Re-Partition (P_T2_CUS_ID, P_I_T1) using P_H_T1 mod 1024
/// 7) Calculate P_H_T2 using P_T2_CUS_NO
/// 5) Re-Partition P_T2_CUS_NO using P_H_T2 mod 1024
/// 6) Re-Partition P_T2_CUS_ID using P_H_T2 mod 1024
/// 7) Calculate Join index (P_T1_J, P_T2_J) using P_T1_CUS_NO, P_T2_CUS_NO
/// 8) Calculate P_C1={(P_T1_CUS_ID, T1.P_T1_J)>=(P_T2_CUS_ID, T1.P_T2_J)}
/// 9) Calculate P_C2={(P_T2_CUS_ID, T1.P_T2_J) IS NULL}
/// 10) Calculate P_C3={P_C1 OR P_C2}
/// 11) Apply to P_T1_J the filter P_C3
/// 12) Apply to P_T2_J the filter P_C3
/// 13) Drop P_T2_CUS_NO --> not needed anymore
/// 13) Calculate NULL vector P_T1_N - in other words valid indexes of P_T1_CUS_ID, which are not listed in P_T1_J
/// 14) If P_T1_N is not empty (otherwise go to 15) )
/// 14.1) Append P_T1_N to P_T1_J
/// 14.2) Insert a NULL at the end of P_T2_CUS_ID
/// 14.3) Append P_T1_N.len() entries in P_T2_J, referencing the NULL row
/// 15) Calculate P_C1={(P_T1_CUS_ID, P_T1_J)>0}
/// 16) Apply filter on P_T1_J using P_C1
/// 17) Apply filter on P_T2_J using P_C1
/// 18) Return Table T3=((P_T1_CUS_NO, P_T1_J),(P_T1_CUS_ID,P_T2_J ))
use std::collections::HashMap;

use crate::table::Table;

#[derive(Debug)]
pub struct Schema<'a> {
    tables: Vec<Table<'a>>,
    names: HashMap<String, usize>,
}
