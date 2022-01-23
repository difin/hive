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

class LockResponse
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'lockid',
            'isRequired' => true,
            'type' => TType::I64,
        ),
        2 => array(
            'var' => 'state',
            'isRequired' => true,
            'type' => TType::I32,
            'class' => '\metastore\LockState',
        ),
        3 => array(
            'var' => 'errorMessage',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
    );

    /**
     * @var int
     */
    public $lockid = null;
    /**
     * @var int
     */
    public $state = null;
    /**
     * @var string
     */
    public $errorMessage = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['lockid'])) {
                $this->lockid = $vals['lockid'];
            }
            if (isset($vals['state'])) {
                $this->state = $vals['state'];
            }
            if (isset($vals['errorMessage'])) {
                $this->errorMessage = $vals['errorMessage'];
            }
        }
    }

    public function getName()
    {
        return 'LockResponse';
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
                    if ($ftype == TType::I64) {
                        $xfer += $input->readI64($this->lockid);
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
                case 3:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->errorMessage);
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
        $xfer += $output->writeStructBegin('LockResponse');
        if ($this->lockid !== null) {
            $xfer += $output->writeFieldBegin('lockid', TType::I64, 1);
            $xfer += $output->writeI64($this->lockid);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->state !== null) {
            $xfer += $output->writeFieldBegin('state', TType::I32, 2);
            $xfer += $output->writeI32($this->state);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->errorMessage !== null) {
            $xfer += $output->writeFieldBegin('errorMessage', TType::STRING, 3);
            $xfer += $output->writeString($this->errorMessage);
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}