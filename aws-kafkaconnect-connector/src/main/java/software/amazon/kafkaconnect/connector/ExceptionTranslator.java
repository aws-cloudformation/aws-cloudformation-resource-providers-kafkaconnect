package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.model.BadRequestException;
import software.amazon.awssdk.services.kafkaconnect.model.ConflictException;
import software.amazon.awssdk.services.kafkaconnect.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.UnauthorizedException;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;

public class ExceptionTranslator {

    public ExceptionTranslator() {
    }

    /**
     * Translation for exceptions coming from SDK having no additional messaging or clarification needs
     * to Cfn exceptions.
     *
     * @param exception SDK exception to translate
     * @param identifier Resource identifying field
     * @return Cfn equivalent exception
     */
    public BaseHandlerException translateToCfnException(
        final AwsServiceException exception,
        final String identifier) {

        if (exception instanceof NotFoundException) {
            return new CfnNotFoundException(ResourceModel.TYPE_NAME, identifier, exception);
        }

        if (exception instanceof BadRequestException) {
            return new CfnInvalidRequestException(exception.getMessage(), exception);
        }

        if (exception instanceof ConflictException) {
            return new CfnAlreadyExistsException(ResourceModel.TYPE_NAME, identifier, exception);
        }

        if (exception instanceof InternalServerErrorException) {
            return new CfnInternalFailureException(exception);
        }

        if (exception instanceof UnauthorizedException) {
            return new CfnAccessDeniedException(ResourceModel.TYPE_NAME, exception);
        }

        return new CfnGeneralServiceException(exception);
    }
}
