use std::{
	error::Error,
	fs::File,
	sync::Arc,
};

use ::arrow::{
	array::{Array, ArrayData, ArrayRef, ListArray, PrimitiveArray},
	buffer::Buffer,
	datatypes::{DataType, Field, Int32Type},
	record_batch::RecordBatch,
};

use parquet::arrow::arrow_writer::ArrowWriter;

pub fn main() -> Result<(), Box<dyn Error>> {
	// make an array like this:
	// [[1, 2, 3], [], [4, 5]]
	let values = PrimitiveArray::<Int32Type>::from_iter_values(vec![1, 2, 3, 4, 5]);
	let list_data_type = DataType::List(Box::new(Field::new("value", DataType::Int32, false)));
	let offsets = Buffer::from_slice_ref(&vec![0i32, 3i32, 3i32, 5i32]);

	let list_data = ArrayData::builder(list_data_type.clone())
		.len(3)
		.add_buffer(offsets)
		.add_child_data(values.data().clone())
		.build();

	let values = ListArray::from(list_data);
	for i in 0 .. 3usize {
		println!("values[{}]: {:?}", i, values.value(i));
	}

	let batch = RecordBatch::try_from_iter_with_nullable(
		vec![("values", Arc::new(values) as ArrayRef, false)]
	)?;

	let buf = File::create("test.parquet")?;
	let mut writer = ArrowWriter::try_new(buf, batch.schema(), None)?;
	writer.write(&batch)?;
	writer.close()?;

	Ok(())
}
