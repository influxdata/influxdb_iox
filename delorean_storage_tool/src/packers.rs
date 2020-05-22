/// This module contains code to pack values into a format suitable for feeding to the parquet writer
use crate::line_protocol_schema;
use parquet::data_type::ByteArray;

// NOTE: See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
// for an explination of nesting levels

// Holds the data for a column of strings
#[derive(Debug)]
pub struct StringPacker {
    pub values: Vec<ByteArray>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl StringPacker {
    fn new() -> StringPacker {
        StringPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                self.values.push(ByteArray::from(s));
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(ByteArray::new());
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

// Holds the data for a column of floats
#[derive(Debug)]
pub struct FloatPacker {
    pub values: Vec<f64>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl FloatPacker {
    fn new() -> FloatPacker {
        FloatPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, f: Option<f64>) {
        match f {
            Some(f) => {
                self.values.push(f);
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(std::f64::NAN); // doesn't matter as def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

// Holds the data for a column of ints
#[derive(Debug)]
pub struct IntPacker {
    pub values: Vec<i64>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl IntPacker {
    fn new() -> IntPacker {
        IntPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, i: Option<i64>) {
        match i {
            Some(i) => {
                self.values.push(i);
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(0); // doesn't matter as def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

#[derive(Debug)]
pub enum Packer {
    StringPackerType(StringPacker),
    FloatPackerType(FloatPacker),
    IntPackerType(IntPacker),
}

impl Packer {
    /// Create a new packer that can pack values of the specified protocol type
    pub fn new(t: line_protocol_schema::LineProtocolType) -> Packer {
        match t {
            line_protocol_schema::LineProtocolType::LPFloat => {
                Packer::FloatPackerType(FloatPacker::new())
            }
            line_protocol_schema::LineProtocolType::LPInteger => {
                Packer::IntPackerType(IntPacker::new())
            }
            line_protocol_schema::LineProtocolType::LPString => {
                Packer::StringPackerType(StringPacker::new())
            }
            line_protocol_schema::LineProtocolType::LPTimestamp => {
                Packer::IntPackerType(IntPacker::new())
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Packer::StringPackerType(string_packer) => string_packer.len(),
            Packer::FloatPackerType(float_packer) => float_packer.len(),
            Packer::IntPackerType(int_packer) => int_packer.len(),
        }
    }

    pub fn pack_str(&mut self, s: Option<&str>) {
        if let Packer::StringPackerType(string_packer) = self {
            string_packer.pack(s)
        } else {
            panic!("Packer {:?} does not know how to pack strings", self);
        }
    }

    pub fn pack_f64(&mut self, f: Option<f64>) {
        if let Packer::FloatPackerType(float_packer) = self {
            float_packer.pack(f)
        } else {
            panic!("Packer {:?} does not know how to pack floats", self);
        }
    }

    pub fn pack_i64(&mut self, i: Option<i64>) {
        if let Packer::IntPackerType(int_packer) = self {
            int_packer.pack(i)
        } else {
            panic!("Packer {:?} does not know how to pack ints", self);
        }
    }

    pub fn as_string_packer(&self) -> &StringPacker {
        if let Packer::StringPackerType(string_packer) = self {
            string_packer
        } else {
            panic!("Packer {:?} is not a string packer", self);
        }
    }

    pub fn as_float_packer(&self) -> &FloatPacker {
        if let Packer::FloatPackerType(float_packer) = self {
            float_packer
        } else {
            panic!("Packer {:?} is not a float packer", self);
        }
    }

    pub fn as_int_packer(&self) -> &IntPacker {
        if let Packer::IntPackerType(int_packer) = self {
            int_packer
        } else {
            panic!("Packer {:?} is not an int packer", self);
        }
    }

    pub fn pack_none(&mut self) {
        match self {
            Packer::StringPackerType(string_packer) => string_packer.pack(None),
            Packer::FloatPackerType(float_packer) => float_packer.pack(None),
            Packer::IntPackerType(int_packer) => int_packer.pack(None),
        }
    }
}
