package ImageProcessingMR;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

public class RecordReaderTest implements RecordReader<Text,BytesWritable>
{

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BytesWritable createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(Text arg0, BytesWritable arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
