///////////////////////////////////////////////////////////////////////////
// C++ code generated with wxFormBuilder (version 4.2.1-0-g80c4cb6)
// http://www.wxformbuilder.org/
//
// PLEASE DO *NOT* EDIT THIS FILE!
///////////////////////////////////////////////////////////////////////////

#include "GLauncherApp.h"

///////////////////////////////////////////////////////////////////////////

MainFrame::MainFrame( wxWindow* parent, wxWindowID id, const wxString& title, const wxPoint& pos, const wxSize& size, long style, const wxString& name ) : wxFrame( parent, id, title, pos, size, style, name )
{
	this->SetSizeHints( wxSize( 100,50 ), wxDefaultSize );

	m_statusBar1 = this->CreateStatusBar( 1, wxSTB_SIZEGRIP, wxID_ANY );
	wxBoxSizer* bSizer3;
	bSizer3 = new wxBoxSizer( wxVERTICAL );

	m_notebook1 = new wxNotebook( this, wxID_ANY, wxDefaultPosition, wxDefaultSize, 0 );
	m_panel1 = new wxPanel( m_notebook1, wxID_ANY, wxDefaultPosition, wxDefaultSize, wxTAB_TRAVERSAL );
	wxBoxSizer* bSizer2;
	bSizer2 = new wxBoxSizer( wxVERTICAL );

	wxStaticBoxSizer* sbSizer4;
	sbSizer4 = new wxStaticBoxSizer( new wxStaticBox( m_panel1, wxID_ANY, _("Status") ), wxVERTICAL );

	wxGridBagSizer* gbSizer1;
	gbSizer1 = new wxGridBagSizer( 0, 0 );
	gbSizer1->SetFlexibleDirection( wxBOTH );
	gbSizer1->SetNonFlexibleGrowMode( wxFLEX_GROWMODE_SPECIFIED );

	m_statusText = new wxStaticText( sbSizer4->GetStaticBox(), wxID_ANY, _("Running"), wxDefaultPosition, wxDefaultSize, 0 );
	m_statusText->Wrap( -1 );
	gbSizer1->Add( m_statusText, wxGBPosition( 0, 0 ), wxGBSpan( 1, 1 ), wxALIGN_CENTER_VERTICAL|wxALL, 5 );

	m_startStopButton = new wxButton( sbSizer4->GetStaticBox(), wxID_ANY, _("Start"), wxDefaultPosition, wxDefaultSize, 0 );
	gbSizer1->Add( m_startStopButton, wxGBPosition( 0, 1 ), wxGBSpan( 1, 1 ), wxALL, 5 );


	sbSizer4->Add( gbSizer1, 1, wxEXPAND, 5 );


	bSizer2->Add( sbSizer4, 1, wxALL|wxEXPAND, 5 );

	wxStaticBoxSizer* sbSizer6;
	sbSizer6 = new wxStaticBoxSizer( new wxStaticBox( m_panel1, wxID_ANY, _("Statistics") ), wxVERTICAL );


	bSizer2->Add( sbSizer6, 1, wxALL|wxEXPAND, 5 );


	m_panel1->SetSizer( bSizer2 );
	m_panel1->Layout();
	bSizer2->Fit( m_panel1 );
	m_notebook1->AddPage( m_panel1, _("Info"), false );
	m_panel3 = new wxPanel( m_notebook1, wxID_ANY, wxDefaultPosition, wxDefaultSize, wxTAB_TRAVERSAL );
	m_notebook1->AddPage( m_panel3, _("Logs"), false );
	m_panel2 = new wxPanel( m_notebook1, wxID_ANY, wxDefaultPosition, wxDefaultSize, wxTAB_TRAVERSAL );
	wxBoxSizer* bSizer5;
	bSizer5 = new wxBoxSizer( wxVERTICAL );

	wxStaticBoxSizer* sbSizer3;
	sbSizer3 = new wxStaticBoxSizer( new wxStaticBox( m_panel2, wxID_ANY, _("JVM") ), wxVERTICAL );

	wxGridBagSizer* gbSizer2;
	gbSizer2 = new wxGridBagSizer( 0, 0 );
	gbSizer2->SetFlexibleDirection( wxBOTH );
	gbSizer2->SetNonFlexibleGrowMode( wxFLEX_GROWMODE_SPECIFIED );

	m_button2 = new wxButton( sbSizer3->GetStaticBox(), wxID_ANY, _("Reset"), wxDefaultPosition, wxDefaultSize, 0 );
	gbSizer2->Add( m_button2, wxGBPosition( 0, 3 ), wxGBSpan( 1, 1 ), wxALL, 5 );


	gbSizer2->Add( 50, 0, wxGBPosition( 0, 2 ), wxGBSpan( 1, 1 ), wxEXPAND, 5 );

	m_javaHomeDirPicker = new wxDirPickerCtrl( sbSizer3->GetStaticBox(), wxID_ANY, wxEmptyString, _("Select a folder"), wxDefaultPosition, wxDefaultSize, wxDIRP_DEFAULT_STYLE|wxDIRP_USE_TEXTCTRL );
	gbSizer2->Add( m_javaHomeDirPicker, wxGBPosition( 0, 1 ), wxGBSpan( 1, 1 ), wxALL|wxEXPAND, 5 );

	m_staticText2 = new wxStaticText( sbSizer3->GetStaticBox(), wxID_ANY, _("Java Home"), wxDefaultPosition, wxDefaultSize, 0 );
	m_staticText2->Wrap( -1 );
	gbSizer2->Add( m_staticText2, wxGBPosition( 0, 0 ), wxGBSpan( 1, 1 ), wxALIGN_CENTER_VERTICAL|wxALL, 5 );


	gbSizer2->AddGrowableCol( 1 );

	sbSizer3->Add( gbSizer2, 1, wxEXPAND, 5 );


	bSizer5->Add( sbSizer3, 1, wxALL|wxEXPAND|wxFIXED_MINSIZE, 5 );

	wxStaticBoxSizer* sbSizer41;
	sbSizer41 = new wxStaticBoxSizer( new wxStaticBox( m_panel2, wxID_ANY, _("Paths") ), wxVERTICAL );

	wxGridBagSizer* gbSizer3;
	gbSizer3 = new wxGridBagSizer( 0, 0 );
	gbSizer3->SetFlexibleDirection( wxBOTH );
	gbSizer3->SetNonFlexibleGrowMode( wxFLEX_GROWMODE_SPECIFIED );

	m_staticText6 = new wxStaticText( sbSizer41->GetStaticBox(), wxID_ANY, _("Mount path"), wxDefaultPosition, wxDefaultSize, 0 );
	m_staticText6->Wrap( -1 );
	gbSizer3->Add( m_staticText6, wxGBPosition( 0, 0 ), wxGBSpan( 1, 1 ), wxALIGN_CENTER_VERTICAL|wxALL, 5 );

	m_mountPathDirPicker = new wxDirPickerCtrl( sbSizer41->GetStaticBox(), wxID_ANY, wxEmptyString, _("Select a folder"), wxDefaultPosition, wxDefaultSize, wxDIRP_DEFAULT_STYLE|wxDIRP_USE_TEXTCTRL );
	gbSizer3->Add( m_mountPathDirPicker, wxGBPosition( 0, 1 ), wxGBSpan( 1, 1 ), wxALL|wxEXPAND, 5 );


	gbSizer3->AddGrowableCol( 1 );

	sbSizer41->Add( gbSizer3, 1, wxEXPAND, 5 );


	bSizer5->Add( sbSizer41, 1, wxALL|wxEXPAND, 5 );


	m_panel2->SetSizer( bSizer5 );
	m_panel2->Layout();
	bSizer5->Fit( m_panel2 );
	m_notebook1->AddPage( m_panel2, _("Settings"), true );
	m_panel4 = new wxPanel( m_notebook1, wxID_ANY, wxDefaultPosition, wxDefaultSize, wxTAB_TRAVERSAL );
	m_notebook1->AddPage( m_panel4, _("Advanced Settings"), false );

	bSizer3->Add( m_notebook1, 1, wxALL|wxEXPAND, 5 );


	this->SetSizer( bSizer3 );
	this->Layout();

	this->Centre( wxBOTH );

	// Connect Events
	m_startStopButton->Connect( wxEVT_COMMAND_BUTTON_CLICKED, wxCommandEventHandler( MainFrame::OnStartStopButtonClick ), NULL, this );
	m_javaHomeDirPicker->Connect( wxEVT_COMMAND_DIRPICKER_CHANGED, wxFileDirPickerEventHandler( MainFrame::OnJavaHomeChanged ), NULL, this );
	m_mountPathDirPicker->Connect( wxEVT_COMMAND_DIRPICKER_CHANGED, wxFileDirPickerEventHandler( MainFrame::OnMountPathChanged ), NULL, this );
}

MainFrame::~MainFrame()
{
}
