///////////////////////////////////////////////////////////////////////////
// C++ code generated with wxFormBuilder (version 4.2.1-0-g80c4cb6)
// http://www.wxformbuilder.org/
//
// PLEASE DO *NOT* EDIT THIS FILE!
///////////////////////////////////////////////////////////////////////////

#pragma once

#include <wx/artprov.h>
#include <wx/xrc/xmlres.h>
#include <wx/intl.h>
#include <wx/statusbr.h>
#include <wx/gdicmn.h>
#include <wx/font.h>
#include <wx/colour.h>
#include <wx/settings.h>
#include <wx/string.h>
#include <wx/stattext.h>
#include <wx/button.h>
#include <wx/bitmap.h>
#include <wx/image.h>
#include <wx/icon.h>
#include <wx/gbsizer.h>
#include <wx/sizer.h>
#include <wx/statbox.h>
#include <wx/panel.h>
#include <wx/textctrl.h>
#include <wx/filepicker.h>
#include <wx/notebook.h>
#include <wx/frame.h>

///////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
/// Class MainFrame
///////////////////////////////////////////////////////////////////////////////
class MainFrame : public wxFrame
{
	private:

	protected:
		wxStatusBar* m_statusBar1;
		wxNotebook* m_notebook1;
		wxPanel* m_panel1;
		wxStaticText* m_statusText;
		wxButton* m_startStopButton;
		wxPanel* m_panel3;
		wxTextCtrl* m_logOutputTextCtrl;
		wxPanel* m_panel2;
		wxButton* m_button2;
		wxDirPickerCtrl* m_javaHomeDirPicker;
		wxStaticText* m_staticText2;
		wxStaticText* m_staticText6;
		wxStaticText* m_staticText61;
		wxDirPickerCtrl* m_mountPathDirPicker;
		wxDirPickerCtrl* m_dataPathDirPicker;
		wxPanel* m_panel4;
		wxPanel* m_panel5;

		// Virtual event handlers, override them in your derived class
		virtual void OnNotebookPageChanged( wxNotebookEvent& event ) { event.Skip(); }
		virtual void OnNotebookPageChanging( wxNotebookEvent& event ) { event.Skip(); }
		virtual void OnStartStopButtonClick( wxCommandEvent& event ) { event.Skip(); }
		virtual void OnJavaHomeChanged( wxFileDirPickerEvent& event ) { event.Skip(); }
		virtual void OnMountPathChanged( wxFileDirPickerEvent& event ) { event.Skip(); }
		virtual void OnDataPathChanged( wxFileDirPickerEvent& event ) { event.Skip(); }


	public:

		MainFrame( wxWindow* parent, wxWindowID id = wxID_ANY, const wxString& title = _("DHFS"), const wxPoint& pos = wxDefaultPosition, const wxSize& size = wxSize( 600,400 ), long style = wxDEFAULT_FRAME_STYLE|wxTAB_TRAVERSAL, const wxString& name = wxT("DHFS") );

		~MainFrame();

};

