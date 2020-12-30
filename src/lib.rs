mod schema;
mod table;

pub use table::*;

#[cfg(test)]
mod tests {

    use crate::{filter, PartitionedIndex, Table, TableExpression};
    use radix_column::*;
    use radix_operations::*;

    #[test]
    fn binarycolumninit() {
        let names = vec![
            "Jane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
        ];
        let bnames = OnwedBinaryColumn::new(&names);

        let datau8: Vec<_> = names
            .iter()
            .map(|s| s.as_bytes())
            .flatten()
            .map(|b| *b)
            .collect();
        let len: Vec<_> = names.iter().map(|s| s.as_bytes().len()).collect();
        let (res_data, _res_start_pos, res_len, _) =
            bnames.downcast_binary_owned::<String>().unwrap();
        assert_eq!(res_data, datau8);
        assert_eq!(res_len, len);
    }

    #[test]
    fn binarycolumn_wrong_type_downcast_owned() {
        let names = vec![
            "Jane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
        ];
        let bnames = OnwedBinaryColumn::new(&names);
        let b = bnames.downcast_binary_owned::<usize>();
        assert_eq!(b.is_err(), true);
    }
    #[test]
    fn binarycolumn_wrong_type_downcast_ref() {
        let names = vec![
            "Jane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
        ];
        let bnames = OnwedBinaryColumn::new(&names);
        let b = bnames.downcast_binary_ref::<usize>();
        assert_eq!(b.is_err(), true);
    }
    #[test]
    fn binarycolumn_wrong_type_downcast_mut() {
        let names = vec![
            "Jane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
        ];
        let mut bnames = OnwedBinaryColumn::new(&names);
        let b = bnames.downcast_binary_mut::<usize>();
        assert_eq!(b.is_err(), true);
    }

    #[test]
    fn binarycolumnlen() {
        let names: Vec<String> = vec![
            "Jane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
        ];
        let dict = Dictionary::new();

        let cw: ColumnWrapper = ColumnWrapper::new_from_slice::<String>(&dict, &names);
        assert_eq!(cw.len(&dict).unwrap(), 4);

        let cw: ColumnWrapper = ColumnWrapper::new_from_vec::<String>(&dict, names);
        assert_eq!(cw.len(&dict).unwrap(), 4);
    }
    #[test]
    fn copycolumnlen() {
        let names: Vec<u32> = vec![1, 2, 3];
        let dict = Dictionary::new();
        let cw: ColumnWrapper = ColumnWrapper::new_from_slice(&dict, &names);
        assert_eq!(cw.len(&dict).unwrap(), 3);

        let cw: ColumnWrapper = ColumnWrapper::new_from_vec(&dict, names);
        assert_eq!(cw.len(&dict).unwrap(), 3);
    }

    #[test]
    fn binarycolumnpartition() {
        let names: Vec<String> = vec![
            "Jaane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
            "Peter".to_string(),
        ];
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 1]);
        t.push(&dict, &names).unwrap();
        let p_column = t.get_part_col(&0).unwrap();

        assert_eq!(p_column.len(), 3);
    }
    #[test]
    fn binarycolumnmaterialize() {
        let names: Vec<String> = vec![
            "Jane".to_string(),
            "Merry".to_string(),
            "".to_string(),
            "Christopher".to_string(),
            "Peter".to_string(),
        ];
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 1]);
        t.push(&dict, &names).unwrap();
        let p_column = t.get_part_col(&0).unwrap();

        assert_eq!(p_column.len(), 3);

        let v = t.materialize::<String>(&dict, &0).unwrap();
        assert_eq!(v.0, names);
        //t.print(&dict).unwrap();
    }
    #[test]
    fn copycolumnmaterialize() {
        let names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);
        t.push(&dict, &names).unwrap();
        let p_column = t.get_part_col(&0).unwrap();

        assert_eq!(p_column.len(), 5);

        let v = t.materialize::<u32>(&dict, &0).unwrap();
        assert_eq!(v.0, names);
        assert!(!v.1.is_some());
    }
    #[test]
    fn copycolumnmaterialize_bitmap() {
        let names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);
        t.push_with_bitmap(&dict, &names, &bitmap).unwrap();
        let p_column = t.get_part_col(&0).unwrap();

        assert_eq!(p_column.len(), 5);

        let v = t.materialize::<u32>(&dict, &0).unwrap();
        assert_eq!(v.0, names);
        assert_eq!(v.1.downcast_ref().unwrap(), bitmap.as_slice());
        //t.print(&dict).unwrap();
    }

    #[test]
    fn columns_addassign() {
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);

        let c1_names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let c1_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];

        let c2_names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let c2_bitmap: Vec<bool> = vec![true, true, true, true, true, true, true, true, false];

        let mut c1 = ColumnWrapper::new_from_vec(&dict, c1_names);
        c1.bitmap_set(ColumnDataF::new(c1_bitmap));

        let mut c2 = ColumnWrapper::new_from_vec(&dict, c2_names);
        c2.bitmap_set(ColumnDataF::new(c2_bitmap));

        c1.op(
            &dict,
            "+=",
            &ColumnDataF::None,
            &[InputTypes::Ref(&c2, &ColumnDataF::None)],
        )
        .unwrap();

        let c1_bitmap = c1.bitmap().downcast_ref().unwrap().to_vec();
        let c2_bitmap = c2.bitmap().downcast_ref().unwrap().to_vec();

        t.push_with_bitmap::<u32>(&dict, c1.column().downcast_ref().unwrap(), &c1_bitmap)
            .unwrap();

        t.push_with_bitmap::<u32>(&dict, c2.column().downcast_ref().unwrap(), &c2_bitmap)
            .unwrap();

        let expected_result = vec!["2", "(null)", "6", "8", "10", "12", "14", "16", "(null)"];
        let result = t.materialize_as_string(&dict, &0).unwrap();
        assert_eq!(result, expected_result);

        //t.print(&dict).unwrap();
    }
    #[test]
    fn columns_eq_copy() {
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);

        let mut c1_names: Vec<bool> = vec![
            false, false, false, false, false, false, false, false, false,
        ];
        let mut c1_bitmap: Vec<bool> = vec![true, true, true, true, false, true, true, true, true];

        let c2_names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let c2_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];
        let c3_names: Vec<u32> = vec![1, 2, 3, 4, 5, 7, 7, 8, 9];
        let c3_bitmap: Vec<bool> = vec![true, true, true, true, true, true, true, true, false];

        t.push_with_bitmap(&dict, &c3_names, &c3_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        t.push_mut_with_bitmap(&mut c1_names, &mut c1_bitmap)
            .unwrap();

        let c3_index: PartitionedIndex = vec![
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0]),
        ];

        t.push_index(c3_index, &[0]).unwrap();

        t.op(&dict, "==", &2, &[1, 0]).unwrap();

        let expected_result = vec![
            "true", "(null)", "true", "false", "true", "false", "true", "false", "(null)",
        ];
        let result = t.materialize_as_string(&dict, &2).unwrap();
        assert_eq!(result, expected_result);

        //t.print(&dict).unwrap();
    }

    #[test]
    fn columns_eq_binary() {
        /*rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();*/
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);

        let mut c1_names: Vec<bool> = vec![
            false, false, false, false, false, false, false, false, false,
        ];
        let mut c1_bitmap: Vec<bool> = vec![true, true, true, true, false, true, true, true, true];

        let c2_names: Vec<String> = vec![
            "1A".to_string(),
            "2A".to_string(),
            "3A".to_string(),
            "4A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "7A".to_string(),
            "8A".to_string(),
            "9A".to_string(),
        ];
        let c2_bitmap: Vec<bool> = vec![true, true, true, true, true, true, true, true, false];
        let c3_names: Vec<String> = vec![
            "1A".to_string(),
            "2A".to_string(),
            "3A".to_string(),
            "4A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "7A".to_string(),
            "8A".to_string(),
            "9A".to_string(),
        ];
        let c3_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];

        t.push_with_bitmap(&dict, &c3_names, &c3_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        t.push_mut_with_bitmap(&mut c1_names, &mut c1_bitmap)
            .unwrap();

        let c2_index: PartitionedIndex = vec![
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0]),
        ];

        t.push_index(c2_index, &[1]).unwrap();

        t.op(&dict, "==", &2, &[1, 0]).unwrap();

        let expected_result = vec![
            "true", "(null)", "true", "false", "true", "false", "true", "false", "(null)",
        ];
        let result = t.materialize_as_string(&dict, &2).unwrap();
        assert_eq!(result, expected_result);
    }
    #[test]
    fn test_filter() {
        let mut index: Vec<usize> = vec![1, 2, 3, 4, 5];
        let keep_all = vec![true, true, true, true, true];
        let keep_some = vec![false, true, false, true, false];

        let mut index_ref = ColumnDataF::new_from_slice_mut(index.as_mut_slice());
        filter(&mut index_ref, &keep_all, &ColumnDataF::None, &None).unwrap();
        assert_eq!(index_ref.downcast_vec().unwrap(), &mut vec![1, 2, 3, 4, 5]);

        let mut index_ref = ColumnDataF::new_from_slice_mut(index.as_mut_slice());
        filter(&mut index_ref, &keep_some, &ColumnDataF::None, &None).unwrap();
        assert_eq!(index_ref.downcast_vec().unwrap(), &mut vec![2, 4]);

        let mut index_owned = ColumnDataF::new(index.clone());
        filter(&mut index_owned, &keep_all, &ColumnDataF::None, &None).unwrap();
        assert_eq!(
            index_owned.downcast_vec().unwrap(),
            &mut vec![1, 2, 3, 4, 5]
        );

        let mut index_owned = ColumnDataF::new(index.clone());
        filter(&mut index_owned, &keep_some, &ColumnDataF::None, &None).unwrap();
        assert_eq!(index_owned.downcast_vec().unwrap(), &mut vec![2, 4]);
    }

    #[test]
    fn columns_filter() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build_global()
            .unwrap();
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);

        let c2_names: Vec<String> = vec![
            "1A".to_string(),
            "2A".to_string(),
            "3A".to_string(),
            "4A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "7A".to_string(),
            "8A".to_string(),
            "9A".to_string(),
        ];
        let c2_bitmap: Vec<bool> = vec![true, true, true, true, true, true, true, true, false];
        let c1_names: Vec<String> = vec![
            "1A".to_string(),
            "2A".to_string(),
            "3A".to_string(),
            "4A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "7A".to_string(),
            "8A".to_string(),
            "9A".to_string(),
        ];
        let c1_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];

        t.push_with_bitmap(&dict, &c1_names, &c1_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        let c2_index: PartitionedIndex = vec![
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0]),
        ];

        t.push_index(c2_index, &[1]).unwrap();

        let e = TableExpression::new("==", &[0, 1]);

        t.filter(&dict, &e).unwrap();
        let expected_result = vec!["1A", "3A", "5A", "7A"];

        let result = t.materialize_as_string(&dict, &1).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_expression() {
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);

        let mut c1_names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut c1_bitmap: Vec<bool> = vec![true, true, true, true, false, true, true, true, true];

        let c2_names: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let c2_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];
        let c3_names: Vec<u32> = vec![1, 2, 3, 4, 5, 7, 7, 8, 9];
        let c3_bitmap: Vec<bool> = vec![true, true, true, true, true, true, true, true, false];

        t.push_with_bitmap(&dict, &c3_names, &c3_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        t.push_mut_with_bitmap(&mut c1_names, &mut c1_bitmap)
            .unwrap();

        let c3_index: PartitionedIndex = vec![
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0, 0]),
            ColumnDataF::new(vec![0]),
        ];

        t.push_index(c3_index, &[0]).unwrap();

        let mut e = TableExpression::new("<", &[500, 1000]);
        e.expand_node(500, "+", &[0, 500]).unwrap();
        e.expand_node(500, "+", &[1, 2]).unwrap();

        let const_val = &ColumnWrapper::new_const(&dict, 16u32);
        e.expand_node_as_const(1000, &mut Some(const_val)).unwrap();

        t.add_expression_as_new_column(&dict, &e);
        //t.print(&dict).unwrap();

        let expected_result = vec![
            "true", "(null)", "true", "true", "(null)", "false", "false", "false", "(null)",
        ];
        let result = t.materialize_as_string(&dict, &3).unwrap();
        assert_eq!(result, expected_result);
    }
}
