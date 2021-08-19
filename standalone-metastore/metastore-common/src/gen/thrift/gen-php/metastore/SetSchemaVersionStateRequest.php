<?php
namespace metastore;

/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;
use Thrift\Type\TMessageType;
use Thrift\Exception\TException;
use Thrift\Exception\TProtocolException;
use Thrift\Protocol\TProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Exception\TApplicationException;

class SetSchemaVersionStateRequest
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'schemaVersion',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\metastore\SchemaVersionDescriptor',
        ),
        2 => array(
            'var' => 'state',
            'isRequired' => false,
            'type' => TType::I32,
            'class' => '\metastore\SchemaVersionState',
        ),
    );

    /**
     * @var \metastore\SchemaVersionDescriptor
     */
    public $schemaVersion = null;
    /**
     * @var int
     */
    public $state = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['schemaVersion'])) {
                $this->schemaVersion = $vals['schemaVersion'];
            }
            if (isset($vals['state'])) {
                $this->state = $vals['state'];
            }
        }
    }

    public function getName()
    {
        return 'SetSchemaVersionStateRequest';
    }


    public function read($input)
    {
        $xfer = 0;
        $fname = null;
        $ftype = 0;
        $fid = 0;
        $xfer += $input->readStructBegin($fname);
        while (true) {
            $xfer += $input->readFieldBegin($fname, $ftype, $fid);
            if ($ftype == TType::STOP) {
                break;
            }
            switch ($fid) {
                case 1:
                    if ($ftype == TType::STRUCT) {
                        $this->schemaVersion = new \metastore\SchemaVersionDescriptor();
                        $xfer += $this->schemaVersion->read($input);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 2:
                    if ($ftype == TType::I32) {
                        $xfer += $input->readI32($this->state);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                default:
                    $xfer += $input->skip($ftype);
                    break;
            }
            $xfer += $input->readFieldEnd();
        }
        $xfer += $input->readStructEnd();
        return $xfer;
    }

    public function write($output)
    {
        $xfer = 0;
        $xfer += $output->writeStructBegin('SetSchemaVersionStateRequest');
        if ($this->schemaVersion !== null) {
            if (!is_object($this->schemaVersion)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('schemaVersion', TType::STRUCT, 1);
            $xfer += $this->schemaVersion->write($output);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->state !== null) {
            $xfer += $output->writeFieldBegin('state', TType::I32, 2);
            $xfer += $output->writeI32($this->state);
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}