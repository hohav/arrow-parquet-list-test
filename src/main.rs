use std::{error::Error, fs::File, sync::Arc};

use ::arrow::{
	array::{ArrayRef, Int32Builder, ListBuilder, StructBuilder},
	datatypes::{DataType, Field},
	record_batch::RecordBatch,
};

use parquet::arrow::arrow_writer::ArrowWriter;

fn list_builder(sb: &mut StructBuilder) -> &mut ListBuilder<Int32Builder> {
	sb.field_builder(0).unwrap()
}

pub fn main() -> Result<(), Box<dyn Error>> {
	let list_type = DataType::List(Box::new(Field::new("id", DataType::Int32, false)));
	let mut sb = StructBuilder::new(
		vec![Field::new("items", list_type, false)],
		vec![Box::new(ListBuilder::new(Int32Builder::new(10)))],
	);

	list_builder(&mut sb).values().append_value(1)?;
	list_builder(&mut sb).append(true)?;
	sb.append(true)?;
	list_builder(&mut sb).append(true)?;
	sb.append(true)?;

	let values = sb.finish();

	let batch = RecordBatch::try_from_iter_with_nullable(
		vec![("values", Arc::new(values) as ArrayRef, false)]
	)?;

	let buf = File::create("test.parquet")?;
	let mut writer = ArrowWriter::try_new(buf, batch.schema(), None)?;
	writer.write(&batch)?;
	writer.close()?;

	Ok(())
}
