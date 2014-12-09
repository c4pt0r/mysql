// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type mysqlConn struct {
	buf              *buffer
	netConn          net.Conn
	affectedRows     uint64
	insertId         uint64
	cfg              *config
	maxPacketAllowed int
	maxWriteSize     int
	flags            clientFlag
	sequence         uint8
	parseTime        bool
	strict           bool
}

type config struct {
	user              string
	passwd            string
	net               string
	addr              string
	dbname            string
	params            map[string]string
	loc               *time.Location
	timeout           time.Duration
	tls               *tls.Config
	allowAllFiles     bool
	allowOldPasswords bool
	clientFoundRows   bool
}

// Handles parameters set in DSN
func (mc *mysqlConn) handleParams() (err error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			charsets := strings.Split(val, ",")
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err = mc.exec("SET NAMES " + charsets[i])
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// time.Time parsing
		case "parseTime":
			var isBool bool
			mc.parseTime, isBool = readBool(val)
			if !isBool {
				return errors.New("Invalid Bool value: " + val)
			}

		// Strict mode
		case "strict":
			var isBool bool
			mc.strict, isBool = readBool(val)
			if !isBool {
				return errors.New("Invalid Bool value: " + val)
			}

		// Compression
		case "compress":
			err = errors.New("Compression not implemented yet")
			return

		// System Vars
		default:
			err = mc.exec("SET " + param + "=" + val + "")
			if err != nil {
				return
			}
		}
	}

	return
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	if mc.netConn == nil {
		errLog.Print(errInvalidConn)
		return nil, driver.ErrBadConn
	}
	err := mc.exec("START TRANSACTION")
	if err == nil {
		return &mysqlTx{mc}, err
	}

	return nil, err
}

func (mc *mysqlConn) Close() (err error) {
	// Makes Close idempotent
	if mc.netConn != nil {
		mc.writeCommandPacket(comQuit)
		mc.netConn.Close()
		mc.netConn = nil
	}

	mc.cfg = nil
	mc.buf = nil

	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	if mc.netConn == nil {
		errLog.Print(errInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := mc.writeCommandPacketStr(comStmtPrepare, query)
	if err != nil {
		return nil, err
	}

	stmt := &mysqlStmt{
		mc: mc,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = mc.readUntilEOF()
		}
	}

	return stmt, err
}

func mysqlEscape(s string) string {
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "\n", "\\n", -1)
	s = strings.Replace(s, "\r", "\\r", -1)
	s = strings.Replace(s, `'`, `\'`, -1)
	s = strings.Replace(s, `"`, `\"`, -1)
	s = strings.Replace(s, "\x1a", "\\Z", -1)
	return s
}

func clientPrepare(query string, args []driver.Value) (string, error) {
	prepStmt := query
	if strings.Count(prepStmt, "?") != len(args) {
		return "", fmt.Errorf("invalid prepare statement")
	}

	prepFmtStmt := strings.Replace(prepStmt, "?", "%s", -1)

	var arguments []interface{}
	for _, arg := range args {
		var valueStr string
		switch arg.(type) {
		case string:
			valueStr = "'" + mysqlEscape(arg.(string)) + "'"
		case int64:
			valueStr = strconv.Itoa(int(arg.(int64)))
		case float64:
			valueStr = strconv.FormatFloat(arg.(float64), 'f', 7, 64)
		case time.Time:
			valueStr = arg.(time.Time).Format("2006-01-02 15:04:05")
		case []byte:
			valueStr = fmt.Sprintf("x'%x'", arg.([]byte))
		default:
			return "", fmt.Errorf("not support type yet")
		}
		arguments = append(arguments, valueStr)
	}

	sql := fmt.Sprintf(prepFmtStmt, arguments...)
	return sql, nil
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if mc.netConn == nil {
		errLog.Print(errInvalidConn)
		return nil, driver.ErrBadConn
	}
	// with args, must use prepared stmt
	// do client side prepare

	query, err := clientPrepare(query, args)
	if err != nil {
		return nil, err
	}

	mc.affectedRows = 0
	mc.insertId = 0

	err = mc.exec(query)
	if err == nil {
		return &mysqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId),
		}, err
	}
	return nil, err
}

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) error {
	// Send command
	err := mc.writeCommandPacketStr(comQuery, query)
	if err != nil {
		return err
	}

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err == nil && resLen > 0 {
		if err = mc.readUntilEOF(); err != nil {
			return err
		}

		err = mc.readUntilEOF()
	}

	return err
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if mc.netConn == nil {
		errLog.Print(errInvalidConn)
		return nil, driver.ErrBadConn
	}

	query, err := clientPrepare(query, args)
	// Send command
	err = mc.writeCommandPacketStr(comQuery, query)
	if err == nil {
		// Read Result
		var resLen int
		resLen, err = mc.readResultSetHeaderPacket()
		if err == nil {
			rows := new(textRows)
			rows.mc = mc

			if resLen > 0 {
				// Columns
				rows.columns, err = mc.readColumns(resLen)
			}
			return rows, err
		}
	}
	return nil, err

}

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func (mc *mysqlConn) getSystemVar(name string) ([]byte, error) {
	// Send command
	if err := mc.writeCommandPacketStr(comQuery, "SELECT @@"+name); err != nil {
		return nil, err
	}

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err == nil {
		rows := new(textRows)
		rows.mc = mc

		if resLen > 0 {
			// Columns
			if err := mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		dest := make([]driver.Value, resLen)
		if err = rows.readRow(dest); err == nil {
			return dest[0].([]byte), mc.readUntilEOF()
		}
	}
	return nil, err
}
