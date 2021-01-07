mod schema;
mod table;

pub use table::*;

#[cfg(test)]
mod tests {

    use crate::{
        filter, tabletotable::TableToTableMap, ExpressionInput, PartitionedIndex, Table,
        TableExpression,
    };
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
            &ColumnDataIndex::None,
            &[InputTypes::Ref(&c2, &ColumnDataIndex::None)],
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
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0]),
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
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0]),
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

        let mut index_ref = ColumnDataIndex::new_from_slice_mut(index.as_mut_slice());
        filter(&mut index_ref, &keep_all, &ColumnDataF::None, &None).unwrap();
        assert_eq!(index_ref.downcast_vec().unwrap(), &mut vec![1, 2, 3, 4, 5]);

        let mut index_ref = ColumnDataIndex::new_from_slice_mut(index.as_mut_slice());
        filter(&mut index_ref, &keep_some, &ColumnDataF::None, &None).unwrap();
        assert_eq!(index_ref.downcast_vec().unwrap(), &mut vec![2, 4]);

        let mut index_owned = ColumnDataIndex::new(index.clone());
        filter(&mut index_owned, &keep_all, &ColumnDataF::None, &None).unwrap();
        assert_eq!(
            index_owned.downcast_vec().unwrap(),
            &mut vec![1, 2, 3, 4, 5]
        );

        let mut index_owned = ColumnDataIndex::new(index.clone());
        filter(&mut index_owned, &keep_some, &ColumnDataF::None, &None).unwrap();
        assert_eq!(index_owned.downcast_vec().unwrap(), &mut vec![2, 4]);
    }

    #[test]
    fn columns_filter() {
        /*rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();*/
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
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0]),
        ];

        t.push_index(c2_index, &[1]).unwrap();
        t.print(&dict).unwrap();
        let e = TableExpression::new("==", &[0, 1]);

        t.filter(&dict, &e).unwrap();
        t.print(&dict).unwrap();
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
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0]),
        ];

        t.push_index(c3_index, &[0]).unwrap();
        t.print(&dict).unwrap();

        let mut e = TableExpression::new("<", &[500, 1000]);
        e.expand_node(500, "+", &[0, 500]).unwrap();
        e.expand_node(500, "+", &[1, 2]).unwrap();

        //(col_0+(col_1+col_2))<16

        let const_val = &ColumnWrapper::new_const(&dict, 16u32);
        e.expand_node_as_const(1000, &mut Some(const_val)).unwrap();

        t.add_expression_as_new_column(&dict, &e);
        t.print(&dict).unwrap();

        let expected_result = vec![
            "true", "(null)", "true", "true", "(null)", "false", "false", "false", "(null)",
        ];
        let result = t.materialize_as_string(&dict, &3).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn columns_hash() {
        /*rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();*/
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
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0]),
        ];

        t.push_index(c2_index, &[1]).unwrap();
        let h1 = t.build_hash(&dict, &[1]);
        let h1: Vec<_> = h1.iter().flatten().map(|i| *i).collect();
        t.push(&dict, &h1).unwrap();

        let h2 = t.build_hash(&dict, &[1, 0]);
        let h2: Vec<_> = h2.iter().flatten().map(|i| *i).collect();
        t.push(&dict, &h2).unwrap();

        assert_eq!(h1[0], h1[1]);
        assert_eq!(h1[2], h1[3]);
        assert_eq!(h1[4], h1[5]);
        assert_eq!(h1[6], h1[7]);
        assert_eq!(h1[8], u64::MAX);

        assert_eq!(h1[1] - 1, h2[1]);

        //t.print(&dict).unwrap();
    }

    #[test]
    fn columns_tabletotable() {
        /*rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();*/
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![2, 2, 2, 2, 1]);

        let c1_names: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let c1_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];

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

        t.push_with_bitmap(&dict, &c1_names, &c1_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        let c2_index: PartitionedIndex = vec![
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0, 0]),
            ColumnDataIndex::new(vec![0]),
        ];

        t.push_index(c2_index, &[1]).unwrap();
        t.print(&dict).unwrap();
        let h = t.build_hash(&dict, &[1]);
        let h1: Vec<_> = h.iter().flatten().map(|i| *i & 3).collect();
        t.push(&dict, &h1).unwrap();

        for (number_of_worker_threads, bucket_bits) in
            vec![(2, 1), (2, 2), (2, 12), (1024, 1), (1024, 8), (1024, 12)]
        {
            let tmap: TableToTableMap =
                TableToTableMap::new(&h, number_of_worker_threads, bucket_bits);
            let res = unsafe { t.column_repartition(&dict, &h, &tmap, &0) };

            let c_part_1: Vec<_> = res
                .iter()
                .map(|c| {
                    let c_data = c.as_string(&dict, &ColumnDataIndex::None).unwrap();
                    let c_bitmap = c.bitmap().downcast_ref().unwrap().to_vec();
                    (c_data, c_bitmap)
                })
                .collect();
            let mut c_part_1: Vec<_> = c_part_1
                .into_iter()
                .map(|(d, b)| d.into_iter().zip(b.into_iter()))
                .flatten()
                .collect();
            c_part_1.sort_by(|a, b| a.0.cmp(&b.0));

            let res = unsafe { t.column_repartition(&dict, &h, &tmap, &1) };

            let c_part_2: Vec<_> = res
                .iter()
                .map(|c| {
                    let c_data = c.as_string(&dict, &ColumnDataIndex::None).unwrap();
                    let c_bitmap = c.bitmap().downcast_ref().unwrap().to_vec();
                    (c_data, c_bitmap)
                })
                .collect();
            let mut c_part_2: Vec<_> = c_part_2
                .into_iter()
                .map(|(d, b)| d.into_iter().zip(b.into_iter()))
                .flatten()
                .collect();
            c_part_2.sort_by(|a, b| a.0.cmp(&b.0));

            let c_part_1_expected = vec![
                ("1".to_string(), true),
                ("2".to_string(), false),
                ("3".to_string(), true),
                ("4".to_string(), true),
                ("5".to_string(), true),
                ("6".to_string(), true),
                ("7".to_string(), true),
                ("8".to_string(), true),
                ("9".to_string(), true),
            ];

            let c_part_2_expected = vec![
                ("1A".to_string(), true),
                ("1A".to_string(), true),
                ("3A".to_string(), true),
                ("3A".to_string(), true),
                ("5A".to_string(), true),
                ("5A".to_string(), true),
                ("7A".to_string(), true),
                ("7A".to_string(), true),
                ("9A".to_string(), false),
            ];

            assert_eq!(c_part_1_expected, c_part_1);
            assert_eq!(c_part_2_expected, c_part_2);
        }
    }
    #[test]
    fn columns_groups() {
        /*rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();*/
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![4, 5]);

        let c1_names: Vec<String> = vec![
            "1A".to_string(),
            "1A".to_string(),
            "3A".to_string(),
            "3A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "7A".to_string(),
            "8A".to_string(),
            "8A".to_string(),
        ];
        let c1_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];

        let c2_names: Vec<String> = vec![
            "1A".to_string(),
            "2A".to_string(),
            "3A".to_string(),
            "4A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "7A".to_string(),
            "8A".to_string(),
            "7A".to_string(),
        ];
        let c2_bitmap: Vec<bool> = vec![true, true, true, true, true, true, false, true, false];

        t.push_with_bitmap(&dict, &c1_names, &c1_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        let c2_index: PartitionedIndex = vec![
            ColumnDataIndex::new(vec![0, 0, 2, 2]),
            ColumnDataIndex::new(vec![0, 0, 2, 2, 4]),
        ];

        t.push_index(c2_index, &[1]).unwrap();
        let h1 = t.build_groups(&dict, &[1]);
        let h1: Vec<_> = h1.iter().map(|(v, _)| v).flatten().map(|i| *i).collect();
        t.push(&dict, &h1).unwrap();

        let h2 = t.build_groups(&dict, &[1, 0]);
        let h2: Vec<_> = h2.iter().map(|(v, _)| v).flatten().map(|i| *i).collect();
        t.push(&dict, &h2).unwrap();

        assert_eq!(h1, vec![0, 0, 1, 1, 0, 0, 1, 1, 1]);
        assert_eq!(h2, vec![0, 1, 2, 2, 0, 1, 2, 3, 3]);

        t.print(&dict).unwrap();
    }
    #[test]
    fn columns_group_expression() {
        /*rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();*/
        let dict = Dictionary::new();

        let mut t: Table = Table::new(vec![4, 5]);

        let c1_names: Vec<String> = vec![
            "1A".to_string(),
            "1A".to_string(),
            "3A".to_string(),
            "3A".to_string(),
            "5A".to_string(),
            "6A".to_string(),
            "5A".to_string(),
            "8A".to_string(),
            "8A".to_string(),
        ];
        let c1_bitmap: Vec<bool> = vec![true, false, true, true, true, true, true, true, true];

        let c2_names: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8, 7];
        let c2_bitmap: Vec<bool> = vec![true, true, true, true, true, true, true, true, false];

        t.push_with_bitmap(&dict, &c1_names, &c1_bitmap).unwrap();

        t.push_with_bitmap(&dict, &c2_names, &c2_bitmap).unwrap();

        let c2_index: PartitionedIndex = vec![
            ColumnDataIndex::new(vec![0, 0, 2, 2]),
            ColumnDataIndex::new(vec![0, 0, 2, 2, 4]),
        ];

        t.push_index(c2_index, &[1]).unwrap();
        let h1 = t.build_groups(&dict, &[1]);
        let h1_group_id: Vec<_> = h1.iter().map(|(v, _)| v).flatten().map(|i| *i).collect();
        t.push(&dict, &h1_group_id).unwrap();
        let h1_num_groups: Vec<Vec<usize>> = h1
            .iter()
            .map(|(v, i)| (0..v.len()).into_iter().map(|_| *i).collect())
            .collect();
        let h1_num_groups: Vec<_> = h1_num_groups.into_iter().flatten().collect();
        t.push(&dict, &h1_num_groups).unwrap();
        let h2 = t.build_groups(&dict, &[1, 0]);
        let h2_group_id: Vec<_> = h2.iter().map(|(v, _)| v).flatten().map(|i| *i).collect();
        t.push(&dict, &h2_group_id).unwrap();

        let h2_num_groups: Vec<Vec<usize>> = h2
            .iter()
            .map(|(v, i)| (0..v.len()).into_iter().map(|_| *i).collect())
            .collect();
        let h2_num_groups: Vec<_> = h2_num_groups.into_iter().flatten().collect();
        t.push(&dict, &h2_num_groups).unwrap();

        t.print(&dict).unwrap();

        let mut e = TableExpression::new("SUM", &[1]);
        e.partition_by.push(ExpressionInput::Column(0));
        //e.expand_node(500, "+", &[0, 500]).unwrap();
        //e.expand_node(500, "+", &[1, 2]).unwrap();

        //let const_val = &ColumnWrapper::new_const(&dict, 16u32);
        //e.expand_node_as_const(1000, &mut Some(const_val)).unwrap();

        t.add_expression_as_new_column(&dict, &e);
        t.print(&dict).unwrap();
        println!("{:?}", t.indexes);
    }

    fn trait_iter() {
        trait ForEach<T, F> {
            fn apply(&mut self, f: F)
            where
                T: Sized,
                F: FnMut(&mut T);
        }

        pub struct A {
            pub data: Vec<u64>,
        }

        pub struct B {
            pub data: Vec<u64>,
            pub index: Vec<usize>,
        }

        impl<F> ForEach<u64, F> for A {
            fn apply(&mut self, f: F)
            where
                F: FnMut(&mut u64),
            {
                #[inline]
                fn call<T>(mut f: impl FnMut(T)) -> impl FnMut((), T) {
                    move |(), item| f(item)
                }

                self.data.iter_mut().fold((), call(f));
            }
        }

        impl<F> ForEach<u64, F> for B {
            fn apply(&mut self, mut f: F)
            where
                F: FnMut(&mut u64),
            {
                #[inline]
                fn call<T>(mut f: impl FnMut(T)) -> impl FnMut((), T) {
                    move |(), item| f(item)
                }
                let (index, data) = (&self.index, &mut self.data);
                index.iter().for_each(|i| f(&mut data[*i]))
            }
        }

        let mut a = A {
            data: vec![1, 2, 3, 4],
        };

        a.apply(|x| *x += 1);

        let mut b = B {
            data: vec![1, 2, 3, 4],
            index: vec![0, 0, 1],
        };

        b.apply(|x| *x += a.data[*x as usize]);

        println!("{:?}", a.data);
        println!("{:?}", b.data);
    }
}
