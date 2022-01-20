package main

import (
	"testing"
)

func TestNewPGTransactionLogger(t *testing.T) {
	type args struct {
		config PGConfig
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "Get New PGTLogger",
			args: args{
				config: PGConfig{
					dbName:   "postgres",
					host:     "localhost",
					user:     "postgres",
					password: "Admin@123",
				},
			},
			wantErr: false,
		},
		{
			name: "Get New PGTLogger Error",
			args: args{
				config: PGConfig{
					dbName:   "postgres",
					host:     "localhost",
					user:     "postgres",
					password: "",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPGTransactionLogger(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPGTransactionLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
