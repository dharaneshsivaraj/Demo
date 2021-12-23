package oracle.apps.sfc.ext.pos.onboard.webui;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import oracle.apps.fnd.framework.OAApplicationModule;
import oracle.apps.fnd.framework.OAException;
import oracle.apps.fnd.framework.server.OADBTransaction;
import oracle.apps.fnd.framework.webui.OAPageContext;
import oracle.apps.fnd.framework.webui.beans.OAWebBean;
import oracle.apps.fnd.framework.webui.beans.message.OAMessageStyledTextBean;
import oracle.apps.pos.onboard.server.FlexRegAMImpl;
import oracle.apps.pos.onboard.webui.ProspRegDetailsPGCO;
import oracle.cabo.style.CSSStyle;

public class SifyExtProspRegDetailsPGCO extends ProspRegDetailsPGCO
{

    public SifyExtProspRegDetailsPGCO()
    {
    }

    public void processRequest(OAPageContext pageContext, OAWebBean webBean)
    {
        FlexRegAMImpl am = (FlexRegAMImpl)pageContext.getApplicationModule(webBean);
        CSSStyle cs = new CSSStyle();
        cs.setProperty("text-transform", "uppercase");
        OAMessageStyledTextBean addTxt = (OAMessageStyledTextBean)webBean.findChildRecursive("CompName");
        if(addTxt != null)
        {
            addTxt.setInlineStyle(cs);
        }
        OAMessageStyledTextBean addTxt1 = (OAMessageStyledTextBean)webBean.findChildRecursive("DUNS");
        if(addTxt1 != null)
        {
            addTxt1.setInlineStyle(cs);
        }
        super.processRequest(pageContext, webBean);
    }
    public void processFormRequest(OAPageContext pageContext, OAWebBean webBean)
    {
        pageContext.writeDiagnostics(this,"Event is :: "+pageContext.getParameter("event"),6);
        pageContext.writeDiagnostics(this,"Source is :: "+pageContext.getParameter("source"),6);
        pageContext.writeDiagnostics(this,"Mapping Id :: "+pageContext.getTransactionValue("mappingId"),6);
        if ("goto".equals(pageContext.getParameter("event"))) {
            pageContext.writeDiagnostics(this,"Goto Event Captured Inside If",6);
           String suppAccpt=validateSupp(pageContext,webBean);
           if("NO".equals(suppAccpt)) {
               throw new OAException("Please Accept the guidelines in Supplier Additional Information page before proceeding further", 
                                     OAException.ERROR);
           }
        }
        super.processFormRequest(pageContext, webBean);
    }
    public String validateSupp(OAPageContext pageContext, 
                                       OAWebBean webBean)
    {
        String supp_accept;
        OADBTransaction oadbtransaction = 
            pageContext.getApplicationModule(webBean).getOADBTransaction();
    try

    {

        Connection conn = 
            pageContext.getApplicationModule(webBean).getOADBTransaction().getJdbcConnection();
        
        String Query = "select nvl(supplier_accepted,'NO') supp_accept from SIFY_SUPP_EXTRA_REGN_INFO where supp_mapping_id=:1";
            //"SELECT sfc_pos_asbn_util_pkg.get_po_ack_status(:1) ack_status from dual";
        
             String mappingId = (String)pageContext.getTransactionValue("mappingId");
        if (oadbtransaction.isLoggingEnabled(6)) {
            oadbtransaction.writeDiagnostics(this, 
                                             "mappingId :: " + mappingId, 6);
        }
        PreparedStatement stmt = conn.prepareStatement(Query);
        stmt.setString(1, mappingId);

        ResultSet resultset = stmt.executeQuery();
        if(resultset.next()) {
            supp_accept = resultset.getString("supp_accept");
            if (oadbtransaction.isLoggingEnabled(6)) {
                oadbtransaction.writeDiagnostics(this, 
                                                 "Result Set Next If :: " + supp_accept, 6);
            }
        }
        else {
            supp_accept="NO";
            if (oadbtransaction.isLoggingEnabled(6)) {
                oadbtransaction.writeDiagnostics(this, 
                                                 "Result Set Next Else :: " + supp_accept, 6);
            }
        }
    } catch (Exception exception)

    {
        throw new OAException("Error in get_po_ack_status Function" + 
                              exception, OAException.ERROR);
    }
    return supp_accept;
    }
}
